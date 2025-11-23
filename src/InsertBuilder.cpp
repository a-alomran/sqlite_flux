// src/InsertBuilder.cpp
#include "InsertBuilder.h"
#include "ValueVisitor.h"
#include <sqlite3.h>
#include <sstream>
#include <stdexcept>
#include <algorithm>

namespace sqlite_flux
{

	// ============================================================================
	// Helper function to convert ColumnValue to SQL string
	// ============================================================================

	static std::string valueToSql(const ColumnValue& value_)
	{
		std::ostringstream oss;

		std::visit(overloaded{
			[&oss](std::monostate) { oss << "NULL"; },
			[&oss](int64_t v) { oss << v; },
			[&oss](double v) { oss << v; },
			[&oss](const std::string& v) {
				oss << "'";
				// SQL escape - replace ' with ''
				for (char c : v)
				{
					if (c == '\'')
						oss << "''";
					else
						oss << c;
				} // end of for
				oss << "'";
			},
			[&oss](const std::vector<uint8_t>&) {
				oss << "?";  // BLOB needs parameter binding
			}
			}, value_);

		return oss.str();
	} // end of valueToSql

	// ============================================================================
	// PreparedInsert Implementation
	// ============================================================================

	PreparedInsert::PreparedInsert(Analyzer& analyzer, const std::string& tableName,
		const std::vector<std::string>& columns,
		ConflictResolution conflict)
		: analyzer_(analyzer), tableName_(tableName), columns_(columns)
	{
		prepareStatement(conflict);

		// Begin transaction for batch operations
		if (!analyzer_.beginTransaction())
		{
			throw std::runtime_error("Failed to begin transaction for batch insert");
		} // end of if
		inTransaction_ = true;
	} // end of PreparedInsert constructor

	PreparedInsert::~PreparedInsert()
	{
		cleanup();
	} // end of PreparedInsert destructor

	PreparedInsert::PreparedInsert(PreparedInsert&& other) noexcept
		: analyzer_(other.analyzer_)
		, tableName_(std::move(other.tableName_))
		, columns_(std::move(other.columns_))
		, stmt_(other.stmt_)
		, inTransaction_(other.inTransaction_)
		, insertCount_(other.insertCount_)
		, batchSize_(other.batchSize_)
	{
		other.stmt_ = nullptr;
		other.inTransaction_ = false;
	} // end of PreparedInsert move constructor

	void PreparedInsert::prepareStatement(ConflictResolution conflict)
	{
		// Build INSERT statement
		std::ostringstream sql;
		sql << "INSERT ";

		// Add conflict resolution clause
		switch (conflict)
		{
		case ConflictResolution::Ignore:
			sql << "OR IGNORE ";
			break;
		case ConflictResolution::Replace:
			sql << "OR REPLACE ";
			break;
		default:
			break;
		} // end of switch

		sql << "INTO " << tableName_ << " (";

		// Column names
		for (size_t i = 0; i < columns_.size(); ++i)
		{
			if (i > 0) sql << ", ";
			sql << columns_[i];
		} // end of for

		sql << ") VALUES (";

		// Placeholders
		for (size_t i = 0; i < columns_.size(); ++i)
		{
			if (i > 0) sql << ", ";
			sql << "?";
		} // end of for

		sql << ")";

		// Prepare statement (note: we need direct access to sqlite3* db)
		// This requires adding a helper method to Analyzer
		// For now, we'll use the execute() approach and optimize later
		// TODO: Add Analyzer::prepareStatement() method for better performance
	} // end of prepareStatement

	PreparedInsert& PreparedInsert::Values(const std::unordered_map<std::string, ColumnValue>& values)
	{
		// Build INSERT statement for this batch item
		std::ostringstream sql;
		sql << "INSERT ";

		// Add conflict resolution clause (we need to store this)
		sql << "INTO " << tableName_ << " (";

		std::vector<ColumnValue> orderedValues;

		// Column names and values in order
		for (size_t i = 0; i < columns_.size(); ++i)
		{
			if (i > 0) sql << ", ";
			sql << columns_[i];

			auto it = values.find(columns_[i]);
			if (it == values.end())
			{
				throw std::runtime_error("Missing value_ for column_: " + columns_[i]);
			} // end of if
			orderedValues.push_back(it->second);
		} // end of for

		sql << ") VALUES (";

		for (size_t i = 0; i < orderedValues.size(); ++i)
		{
			if (i > 0) sql << ", ";
			sql << valueToSql(orderedValues[i]);
		} // end of for

		sql << ")";

		// Store for execution
		currentSql_ = sql.str();

		return *this;
	} // end of Values

	void PreparedInsert::ExecuteBatch()
	{
		if (currentSql_.empty())
		{
			throw std::runtime_error("No values set for batch insert");
		} // end of if

		// Execute the insert
		if (!analyzer_.execute(currentSql_))
		{
			throw std::runtime_error("Batch insert failed: " + analyzer_.getLastError());
		} // end of if

		++insertCount_;
		currentSql_.clear();

		// Auto-commit every N operations for better performance
		if (insertCount_ % batchSize_ == 0)
		{
			analyzer_.commit();
			analyzer_.beginTransaction();
		} // end of if
	} // end of ExecuteBatch

	int64_t PreparedInsert::Finalize()
	{
		if (inTransaction_)
		{
			if (!analyzer_.commit())
			{
				analyzer_.rollback();
				throw std::runtime_error("Failed to commit batch insert transaction");
			} // end of if
			inTransaction_ = false;
		} // end of if

		cleanup();
		return insertCount_;
	} // end of Finalize

	void PreparedInsert::cleanup()
	{
		if (inTransaction_)
		{
			analyzer_.rollback();
			inTransaction_ = false;
		} // end of if

		if (stmt_)
		{
			sqlite3_finalize(stmt_);
			stmt_ = nullptr;
		} // end of if
	} // end of cleanup

	// ============================================================================
	// InsertBuilder Implementation
	// ============================================================================

	InsertBuilder::InsertBuilder(Analyzer& analyzer, const std::string& tableName)
		: analyzer_(analyzer), tableName_(tableName)
	{
		// Get and cache table schema
		schema_ = analyzer_.getTableSchema(tableName_);

		if (schema_.empty())
		{
			throw std::runtime_error("Table not found or has no columns: " + tableName_);
		} // end of if
	} // end of InsertBuilder constructor

	InsertBuilder& InsertBuilder::Values(const std::unordered_map<std::string, ColumnValue>& values)
	{
		values_ = values;

		// Validate all columns
		validateAllColumns();

		return *this;
	} // end of Values

	InsertBuilder& InsertBuilder::OrIgnore()
	{
		conflictResolution_ = ConflictResolution::Ignore;
		return *this;
	} // end of OrIgnore

	InsertBuilder& InsertBuilder::OrReplace()
	{
		conflictResolution_ = ConflictResolution::Replace;
		return *this;
	} // end of OrReplace

	int64_t InsertBuilder::Execute()
	{
		if (values_.empty())
		{
			throw std::runtime_error("No values set for insert");
		} // end of if

		std::string sql = buildSql();

		if (!analyzer_.execute(sql))
		{
			throw std::runtime_error("Insert failed: " + analyzer_.getLastError());
		} // end of if

		// Get last insert rowid
		// Note: This requires adding a helper method to Analyzer
		// For now, we'll query it
		auto result = analyzer_.query("SELECT last_insert_rowid() as rowid");
		if (!result.empty())
		{
			auto rowid = getValue<int64_t>(result[0], "rowid");
			return rowid.value_or(0);
		} // end of if

		return 0;
	} // end of Execute

	PreparedInsert InsertBuilder::Prepare()
	{
		if (values_.empty())
		{
			throw std::runtime_error("No values set for prepared insert");
		} // end of if

		// Extract column names from values
		std::vector<std::string> columns;
		for (const auto& [col, val] : values_)
		{
			columns.push_back(col);
		} // end of for

		return PreparedInsert(analyzer_, tableName_, columns, conflictResolution_);
	} // end of Prepare

	std::string InsertBuilder::buildSql() const
	{
		std::ostringstream sql;

		sql << "INSERT " << getConflictClause();
		sql << "INTO " << tableName_ << " (";

		// Column names
		size_t index = 0;
		for (const auto& [col, val] : values_)
		{
			if (index++ > 0) sql << ", ";
			sql << col;
		} // end of for

		sql << ") VALUES (";

		// Values
		index = 0;
		for (const auto& [col, val] : values_)
		{
			if (index++ > 0) sql << ", ";
			sql << valueToSql(val);
		} // end of for

		sql << ")";

		return sql.str();
	} // end of buildSql

	void InsertBuilder::validateColumn(const std::string& column_) const
	{
		auto it = std::find_if(schema_.begin(), schema_.end(), [&column_](const ColumnInfo& info) { return info.name == column_; });

		if (it == schema_.end())
		{
			throw std::runtime_error("Column '" + column_ + "' not found in table '" + tableName_ + "'");
		} // end of if
	} // end of validateColumn

	void InsertBuilder::validateColumnType(const std::string& column_, const ColumnValue& value_) const
	{
		// Allow NULL for any column type
		if (std::holds_alternative<std::monostate>(value_))
		{
			return;
		} // end of if

		std::string sqliteType = getSqliteType(column_);

		if (!isTypeCompatible(sqliteType, value_))
		{
			std::string valueType;
			std::visit(overloaded{
				[&valueType](std::monostate) { valueType = "NULL"; },
				[&valueType](int64_t) { valueType = "int64_t"; },
				[&valueType](double) { valueType = "double"; },
				[&valueType](const std::string&) { valueType = "string"; },
				[&valueType](const std::vector<uint8_t>&) { valueType = "blob"; }
				}, value_);

			throw std::runtime_error(	"Type mismatch for column_ '" + column_ + "': " +"expected " + sqliteType + ", got " + valueType);
		} // end of if
	} // end of validateColumnType

	void InsertBuilder::validateAllColumns() const
	{
		for (const auto& [col, val] : values_)
		{
			validateColumn(col);
			validateColumnType(col, val);
		} // end of for
	} // end of validateAllColumns

	std::string InsertBuilder::getSqliteType(const std::string& column_) const
	{
		auto it = std::find_if(schema_.begin(), schema_.end(),
			[&column_](const ColumnInfo& info) { return info.name == column_; });

		if (it != schema_.end())
		{
			return it->type;
		} // end of if

		return "";
	} // end of getSqliteType

	bool InsertBuilder::isTypeCompatible(const std::string& sqliteType, const ColumnValue& value_) const
	{
		// SQLite type affinity rules
		// INTEGER affinity
		if (sqliteType.find("INT") != std::string::npos)
		{
			return std::holds_alternative<int64_t>(value_);
		} // end of if

		// TEXT affinity
		if (sqliteType.find("CHAR") != std::string::npos ||
			sqliteType.find("CLOB") != std::string::npos ||
			sqliteType.find("TEXT") != std::string::npos)
		{
			return std::holds_alternative<std::string>(value_);
		} // end of if

		// BLOB affinity
		if (sqliteType.find("BLOB") != std::string::npos)
		{
			return std::holds_alternative<std::vector<uint8_t>>(value_);
		} // end of if

		// REAL affinity
		if (sqliteType.find("REAL") != std::string::npos ||
			sqliteType.find("FLOA") != std::string::npos ||
			sqliteType.find("DOUB") != std::string::npos)
		{
			return std::holds_alternative<double>(value_);
		} // end of if

		// NUMERIC affinity - accepts both int and real
		return std::holds_alternative<int64_t>(value_) ||
			std::holds_alternative<double>(value_);
	} // end of isTypeCompatible

	std::string InsertBuilder::getConflictClause() const
	{
		switch (conflictResolution_)
		{
		case ConflictResolution::Ignore:
			return "OR IGNORE ";
		case ConflictResolution::Replace:
			return "OR REPLACE ";
		default:
			return "";
		} // end of switch
	} // end of getConflictClause

} // namespace sqlite_flux