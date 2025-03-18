#include "duckdb/catalog/catalog_entry/table_function_catalog_entry.hpp"
#include "duckdb/common/multi_file/multi_file_reader.hpp"
#include "duckdb/main/extension_util.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"
#include "pgduckdb/catalog/mooncake_table.hpp"

using namespace duckdb;

namespace pgduckdb {

struct MooncakeFunctionInfo : public TableFunctionInfo {
	MooncakeFunctionInfo(MooncakeTable &_table) : table(_table) {
	}

	MooncakeTable &table;
};

struct MooncakeMultiFileList : public MultiFileList {
	MooncakeMultiFileList(MooncakeTable &_table)
	    : MultiFileList({}, FileGlobOptions::ALLOW_EMPTY), table(_table), metadata(nullptr) {
	}

	// Lazily initialized because pg_duckdb binds each query three times
	void
	LazyInitialize() {
		metadata = &table.GetTableMetadata();
	}

	vector<OpenFileInfo>
	GetAllFiles() override {
		throw NotImplementedException("GetAllFiles not supported yet");
	}

	FileExpandResult
	GetExpandResult() override {
		return FileExpandResult::MULTIPLE_FILES;
	}

	idx_t
	GetTotalFileCount() override {
		return metadata ? metadata->GetNumDataFiles() : 0;
	}

	unique_ptr<MultiFileList>
	Copy() override {
		return make_uniq<MooncakeMultiFileList>(table);
	}

	OpenFileInfo
	GetFile(idx_t i) override {
		return metadata && i < metadata->GetNumDataFiles() ? metadata->GetDataFile(i) : "";
	}

	MooncakeTable &table;
	optional_ptr<MooncakeTableMetadata> metadata;
};

struct MooncakeMultiFileReader : public MultiFileReader {
	MooncakeMultiFileReader(MooncakeTable &_table) : table(_table) {
	}

	static unique_ptr<MultiFileReader>
	Create(const TableFunction &table_function) {
		return make_uniq<MooncakeMultiFileReader>(table_function.function_info->Cast<MooncakeFunctionInfo>().table);
	}

	shared_ptr<MultiFileList>
	CreateFileList(ClientContext &, const vector<string> &, FileGlobOptions) override {
		return make_shared_ptr<MooncakeMultiFileList>(table);
	}

	bool
	Bind(MultiFileOptions &, MultiFileList &, vector<LogicalType> &return_types, vector<string> &names,
	     MultiFileReaderBindData &) override {
		for (auto &column : table.GetColumns().Logical()) {
			return_types.emplace_back(column.GetType());
			names.emplace_back(column.GetName());
		}
		return true;
	}

	ReaderInitializeType
	InitializeReader(MultiFileReaderData &reader_data, const MultiFileBindData &bind_data,
	                 const vector<MultiFileColumnDefinition> &global_columns,
	                 const vector<ColumnIndex> &global_column_ids, optional_ptr<TableFilterSet> table_filters,
	                 ClientContext &context, MultiFileGlobalState &gstate) override {
		uint32_t data_file_number = reader_data.reader->file_list_idx.GetIndex();
		reader_data.reader->deletion_filter = table.GetTableMetadata().GetDeleteFilter(context, data_file_number);
		return MultiFileReader::InitializeReader(reader_data, bind_data, global_columns, global_column_ids,
		                                         table_filters, context, gstate);
	}

	MooncakeTable &table;
};

static TableFunction &
GetParquetScan(ClientContext &context) {
	return ExtensionUtil::GetTableFunction(*context.db, "parquet_scan").functions.GetFunctionReferenceByOffset(0);
}

static unique_ptr<GlobalTableFunctionState>
MooncakeScanInitGlobal(ClientContext &context, TableFunctionInitInput &input) {
	input.bind_data->Cast<MultiFileBindData>().file_list->Cast<MooncakeMultiFileList>().LazyInitialize();
	return GetParquetScan(context).init_global(context, input);
}

static InsertionOrderPreservingMap<string>
MooncakeScanToString(TableFunctionToStringInput &input) {
	InsertionOrderPreservingMap<string> result;
	result["Table"] = input.table_function.function_info->Cast<MooncakeFunctionInfo>().table.name;
	return result;
}

static BindInfo
MooncakeScanGetBindInfo(const optional_ptr<FunctionData> bind_data) {
	return BindInfo(bind_data->Cast<MultiFileBindData>().file_list->Cast<MooncakeMultiFileList>().table);
}

TableFunction
MooncakeTable::GetScanFunction(ClientContext &context, unique_ptr<FunctionData> &bind_data) {
	TableFunction mooncake_scan = GetParquetScan(context);
	mooncake_scan.name = "mooncake_scan";
	mooncake_scan.init_global = MooncakeScanInitGlobal;
	mooncake_scan.to_string = MooncakeScanToString;
	mooncake_scan.get_bind_info = MooncakeScanGetBindInfo;
	mooncake_scan.get_multi_file_reader = MooncakeMultiFileReader::Create;
	mooncake_scan.function_info = make_shared_ptr<MooncakeFunctionInfo>(*this);
	vector<Value> inputs {""};
	named_parameter_map_t named_parameters;
	vector<LogicalType> input_table_types;
	vector<string> input_table_names;
	TableFunctionBindInput bind_input(inputs, named_parameters, input_table_types, input_table_names, nullptr /*info*/,
	                                  nullptr /*binder*/, mooncake_scan, {} /*ref*/);
	vector<LogicalType> return_types;
	vector<string> names;
	bind_data = mooncake_scan.bind(context, bind_input, return_types, names);
	return mooncake_scan;
}

} // namespace pgduckdb
