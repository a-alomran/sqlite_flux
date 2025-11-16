#pragma once

#include "Analyzer.h"
#include "TableTypes.h"
#include "ColumnValue.h"
#include <string>
#include <vector>
#include <memory>

namespace sqlite_flux
{

// ============================================================================
// CompareOp - SQL comparison operators
// ============================================================================

enum class CompareOp
{
    Equal,
    NotEqual,
    LessThan,
    LessThanOrEqual,
    GreaterThan,
    GreaterThanOrEqual,
    Like,
    In
};

// ============================================================================
// FilterCondition - Represents a single WHERE condition
// ============================================================================

struct FilterCondition
{
    std::string column;
    ColumnValue value;
    CompareOp op;

    FilterCondition(const std::string& col, const ColumnValue& val, CompareOp operation = CompareOp::Equal);
    
    std::string toSql() const;
};

// ============================================================================
// QueryBuilder - Fluent interface for building SELECT queries
// ============================================================================

class QueryBuilder
{
public:
    QueryBuilder(Analyzer& kdb, const std::string& tableName);

    // Variadic template for column selection
    template<typename... Cols>
    QueryBuilder& Columns(Cols&&... cols);

    // Filter methods
    QueryBuilder& Filter(const std::string& column, const ColumnValue& value);
    QueryBuilder& Filter(const std::string& column, const ColumnValue& value, CompareOp op);

    // Ordering
    QueryBuilder& OrderBy(const std::string& column, bool ascending = true);

    // Pagination
    QueryBuilder& Limit(int limit);
    QueryBuilder& Offset(int offset);

    // Execution methods
    ResultSet Execute();
    std::optional<Row> ExecuteFirst();
    
    template<typename T>
    std::optional<T> ExecuteScalar();
    
    int64_t Count();
    bool Any();

    // SQL generation (for debugging)
    std::string buildSql() const;

private:
    Analyzer& kdb_;
    std::string tableName_;
    TableSchema schema_;
    std::vector<std::string> selectedColumns_;
    std::vector<FilterCondition> filters_;
    std::string orderByColumn_;
    bool orderAscending_ = true;
    int limitValue_ = -1;
    int offsetValue_ = -1;

    // Validation helpers
    void validateColumn(const std::string& column) const;
    void validateColumnType(const std::string& column, const ColumnValue& value) const;
    std::string getSqliteType(const std::string& column) const;
    bool isTypeCompatible(const std::string& sqliteType, const ColumnValue& value) const;
};

// ============================================================================
// QueryFactory - Factory for creating QueryBuilder instances
// ============================================================================

class QueryFactory
{
public:
    explicit QueryFactory(Analyzer& kdb);

    QueryBuilder FromTable(const std::string& tableName);

private:
    Analyzer& kdb_;
};

// ============================================================================
// Template implementations
// ============================================================================

template<typename... Cols>
QueryBuilder& QueryBuilder::Columns(Cols&&... cols)
{
    selectedColumns_.clear();
    (selectedColumns_.push_back(std::forward<Cols>(cols)), ...);

    // Validate all columns exist in schema
    for (const auto& col : selectedColumns_)
    {
        validateColumn(col);
    }

    return *this;
}

template<typename T>
std::optional<T> QueryBuilder::ExecuteScalar()
{
    auto results = Execute();
    
    if (results.empty() || results[0].empty())
    {
        return std::nullopt;
    }

    const auto& firstRow = results[0];
    const auto& firstColumn = firstRow.begin()->second;

    if (auto* val = std::get_if<T>(&firstColumn))
    {
        return *val;
    }

    return std::nullopt;
}

} // namespace sqlite_flux
