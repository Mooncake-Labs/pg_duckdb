#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_function_catalog_entry.hpp"
#include "duckdb/common/multi_file_reader.hpp"
#include "duckdb/main/extension_util.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"
#include "pgduckdb/scan/mooncake_table_reader.hpp"

extern "C" {
#include "postgres.h"
}

using namespace duckdb;

namespace pgduckdb {

struct MooncakeFunctionInfo : public TableFunctionInfo {
	MooncakeFunctionInfo(TableCatalogEntry &_table, MooncakeTableReader &_reader) : table(_table), reader(_reader) {
	}

	TableCatalogEntry &table;
	MooncakeTableReader &reader;
};

class MooncakeMultiFileList : public MultiFileList {
public:
	MooncakeMultiFileList() : MultiFileList({""}, FileGlobOptions::ALLOW_EMPTY), metadata(nullptr) {
	}

	void
	LazyInitialize(MooncakeTableMetadata *_metadata) {
		metadata = _metadata;
	}

	vector<string>
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

protected:
	string
	GetFile(idx_t i) override {
		return metadata && i < metadata->GetNumDataFiles() ? metadata->GetDataFile(i) : "";
	}

private:
	MooncakeTableMetadata *metadata;
};

class MooncakeMultiFileReader : public MultiFileReader {
public:
	MooncakeMultiFileReader(TableCatalogEntry &_table, MooncakeTableReader &_reader)
	    : table(_table), reader(_reader), metadata(nullptr) {
	}

	static unique_ptr<MultiFileReader>
	Create(const TableFunction &table_function) {
		auto function_info = table_function.function_info->Cast<MooncakeFunctionInfo>();
		return make_uniq<MooncakeMultiFileReader>(function_info.table, function_info.reader);
	}

	shared_ptr<MultiFileList>
	CreateFileList(ClientContext &, const vector<string> &, FileGlobOptions) override {
		file_list = make_shared_ptr<MooncakeMultiFileList>();
		return file_list;
	}

	bool
	Bind(MultiFileReaderOptions &, MultiFileList &, vector<LogicalType> &return_types, vector<string> &names,
	     MultiFileReaderBindData &bind_data) {
		for (auto &column : table.GetColumns().Logical()) {
			return_types.emplace_back(column.GetType());
			names.emplace_back(column.GetName());
		}
		bind_data.file_row_number_idx = names.size();
		return true;
	}

	unique_ptr<MultiFileList>
	DynamicFilterPushdown(ClientContext &, const MultiFileList &, const MultiFileReaderOptions &,
	                      const vector<string> &, const vector<LogicalType> &, const vector<column_t> &,
	                      TableFilterSet &) override {
		// TODO: segment elimination
		return nullptr;
	}

	unique_ptr<MultiFileReaderGlobalState>
	InitializeGlobalState(ClientContext &context, const MultiFileReaderOptions &, const MultiFileReaderBindData &,
	                      const MultiFileList &, const vector<MultiFileReaderColumnDefinition> &,
	                      const vector<ColumnIndex> &) {
		LazyInitialize(context);
		vector<LogicalType> extra_columns {LogicalType::BIGINT};
		return make_uniq<MultiFileReaderGlobalState>(extra_columns, *file_list);
	}

	void
	CreateColumnMapping(const string &, const vector<MultiFileReaderColumnDefinition> &,
	                    const vector<MultiFileReaderColumnDefinition> &, const vector<ColumnIndex> &global_column_ids,
	                    MultiFileReaderData &reader_data, const MultiFileReaderBindData &bind_data, const string &,
	                    optional_ptr<MultiFileReaderGlobalState>) override {
		for (idx_t i = 0; i < global_column_ids.size(); i++) {
			if (!global_column_ids[i].IsRowIdColumn()) {
				reader_data.column_mapping.push_back(i);
				reader_data.column_ids.push_back(global_column_ids[i].GetPrimaryIndex());
			}
		}
		if (metadata->HasDeletes(reader_data.file_list_idx.GetIndex())) {
			reader_data.column_mapping.push_back(global_column_ids.size());
			reader_data.column_ids.push_back(bind_data.file_row_number_idx);
		}
	}

	void
	FinalizeChunk(ClientContext &context, const MultiFileReaderBindData &bind_data,
	              const MultiFileReaderData &reader_data, DataChunk &chunk,
	              optional_ptr<MultiFileReaderGlobalState> global_state) override {
		MultiFileReader::FinalizeChunk(context, bind_data, reader_data, chunk, global_state);
		auto data_file_number = reader_data.file_list_idx.GetIndex();
		auto deletes = metadata->HasDeletes(data_file_number);
		if (deletes) {
			idx_t size = chunk.size();
			UnifiedVectorFormat data;
			chunk.data[chunk.ColumnCount() - 1].ToUnifiedFormat(size, data);
			auto data_file_row_numbers = UnifiedVectorFormat::GetData<int64_t>(data);
			SelectionVector sel(size);
			idx_t count = 0;
			for (idx_t i = 0; i < size; i++) {
				auto data_file_row_number = data_file_row_numbers[data.sel->get_index(i)];
				// TODO: Replace with DeleteFilter once pg_duckdb upgrades to DuckDB v1.3.0
				if (!metadata->IsDeleted(data_file_number, data_file_row_number)) {
					sel.set_index(count++, i);
				}
			}
			if (count != size) {
				chunk.Slice(sel, count);
			}
		}
	}

private:
	// Lazily initialized because pg_duckdb binds each query three times
	void
	LazyInitialize(ClientContext &context) {
		metadata = &reader.GetTableMetadata(context);
		file_list->LazyInitialize(metadata);
	}

	TableCatalogEntry &table;
	MooncakeTableReader &reader;
	shared_ptr<MooncakeMultiFileList> file_list;
	MooncakeTableMetadata *metadata;
};

static InsertionOrderPreservingMap<string>
MooncakeScanFunctionToString(TableFunctionToStringInput &input) {
	InsertionOrderPreservingMap<string> result;
	result["Table"] = input.table_function.function_info->Cast<MooncakeFunctionInfo>().table.name;
	return result;
}

TableFunction
GetMooncakeScanFunction(ClientContext &context, TableCatalogEntry &table, MooncakeTableReader &reader,
                        idx_t cardinality, unique_ptr<FunctionData> &bind_data) {
	TableFunction mooncake_scan = ExtensionUtil::GetTableFunction(*context.db, "parquet_scan")
	                                  .functions.GetFunctionByArguments(context, {LogicalType::VARCHAR});
	mooncake_scan.name = "mooncake_scan";
	mooncake_scan.to_string = MooncakeScanFunctionToString;
	mooncake_scan.get_bind_info = nullptr;
	mooncake_scan.get_multi_file_reader = MooncakeMultiFileReader::Create;
	mooncake_scan.function_info = make_shared_ptr<MooncakeFunctionInfo>(table, reader);
	vector<Value> inputs {""};
	named_parameter_map_t named_parameters {{"explicit_cardinality", Value::UBIGINT(cardinality)}};
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
