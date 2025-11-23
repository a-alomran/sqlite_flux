// src/QueryBuilder.cpp
#include "QueryBuilder.h"
#include "InsertBuilder.h"
#include "UpdateBuilder.h"
#include "DeleteBuilder.h"
#include "ValueVisitor.h"
#include <sstream>
#include <algorithm>
#include <stdexcept>

namespace sqlite_flux
{

    // ============================================================================
    // Helper function to convert CompareOp to SQL string
    // ============================================================================

    static std::string compareOpToString(CompareOp op_)
    {
        switch (op_)
        {
        case CompareOp::Equal:              return "=";
        case CompareOp::NotEqual:           return "!=";
        case CompareOp::LessThan:           return "<";
        case CompareOp::LessThanOrEqual:    return "<=";
        case CompareOp::GreaterThan:        return ">";
        case CompareOp::GreaterThanOrEqual: return ">=";
        case CompareOp::Like:               return "LIKE";
        case CompareOp::In:                 return "IN";
        default:                            return "=";
        }
    }

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
                // Basic SQL escape - replace ' with ''
                for (char c : v)
                {
                    if (c == '\'')
                        oss << "''";
                    else
                        oss << c;
                }
                oss << "'";
            },
            [&oss](const std::vector<uint8_t>&) {
                oss << "?";  // BLOB needs parameter binding
            }
            }, value_);

        return oss.str();
    }

    // ============================================================================
    // FilterCondition Implementation
    // ============================================================================

    FilterCondition::FilterCondition(const std::string& col, const ColumnValue& val, CompareOp operation)
        : column_(col), value_(val), op_(operation)
    {
    }

    std::string FilterCondition::toSql() const
    {
        std::ostringstream oss;
        oss << column_ << " " << compareOpToString(op_) << " " << valueToSql(value_);
        return oss.str();
    }

    // ============================================================================
    // QueryBuilder Implementation
    // ============================================================================

    QueryBuilder::QueryBuilder(Analyzer& analyzer, const std::string& tableName)
        : analyzer_(analyzer), tableName_(tableName)
    {
        // Get and cache the table schema
        schema_ = analyzer_.getTableSchema(tableName_);

        if (schema_.empty())
        {
            throw std::runtime_error("Table not found or has no columns: " + tableName_);
        }
    }

    QueryBuilder& QueryBuilder::Filter(const std::string& column_, const ColumnValue& value_)
    {
        return Filter(column_, value_, CompareOp::Equal);
    }

    QueryBuilder& QueryBuilder::Filter(const std::string& column_, const ColumnValue& value_, CompareOp op_)
    {
        // Validate column exists
        validateColumn(column_);

        // Validate type compatibility
        validateColumnType(column_, value_);

        // Add filter condition
        filters_.emplace_back(column_, value_, op_);

        return *this;
    }

    QueryBuilder& QueryBuilder::OrderBy(const std::string& column_, bool ascending)
    {
        // Validate column exists
        validateColumn(column_);

        orderByColumn_ = column_;
        orderAscending_ = ascending;

        return *this;
    }

    QueryBuilder& QueryBuilder::Limit(int limit)
    {
        if (limit < 0)
        {
            throw std::invalid_argument("Limit must be non-negative");
        }

        limitValue_ = limit;
        return *this;
    }

    QueryBuilder& QueryBuilder::Offset(int offset)
    {
        if (offset < 0)
        {
            throw std::invalid_argument("Offset must be non-negative");
        }

        offsetValue_ = offset;
        return *this;
    }

    ResultSet QueryBuilder::Execute()
    {
        std::string sql = buildSql();
        return analyzer_.query(sql);
    }

    std::optional<Row> QueryBuilder::ExecuteFirst()
    {
        // Temporarily set limit to 1
        int originalLimit = limitValue_;
        limitValue_ = 1;

        auto results = Execute();

        // Restore original limit
        limitValue_ = originalLimit;

        if (results.empty())
        {
            return std::nullopt;
        }

        return results[0];
    }

    int64_t QueryBuilder::Count()
    {
        // Save current state
        auto originalColumns = selectedColumns_;
        int originalLimit = limitValue_;
        int originalOffset = offsetValue_;

        // Modify for COUNT query
        selectedColumns_.clear();
        selectedColumns_.push_back("COUNT(*) as count");
        limitValue_ = -1;
        offsetValue_ = -1;

        // Execute
        auto results = Execute();

        // Restore state
        selectedColumns_ = originalColumns;
        limitValue_ = originalLimit;
        offsetValue_ = originalOffset;

        if (results.empty())
        {
            return 0;
        }

        auto count = getValue<int64_t>(results[0], "count");
        return count.value_or(0);
    }

    bool QueryBuilder::Any()
    {
        return Count() > 0;
    }

    std::string QueryBuilder::buildSql() const
    {
        std::ostringstream sql;

        // SELECT clause
        sql << "SELECT ";
        if (selectedColumns_.empty())
        {
            sql << "*";
        }
        else
        {
            for (size_t i = 0; i < selectedColumns_.size(); ++i)
            {
                if (i > 0) sql << ", ";
                sql << selectedColumns_[i];
            }
        }

        // FROM clause
        sql << " FROM " << tableName_;

        // WHERE clause
        if (!filters_.empty())
        {
            sql << " WHERE ";
            for (size_t i = 0; i < filters_.size(); ++i)
            {
                if (i > 0) sql << " AND ";
                sql << filters_[i].toSql();
            }
        }

        // ORDER BY clause
        if (!orderByColumn_.empty())
        {
            sql << " ORDER BY " << orderByColumn_;
            sql << (orderAscending_ ? " ASC" : " DESC");
        }

        // LIMIT clause
        if (limitValue_ > 0)
        {
            sql << " LIMIT " << limitValue_;
        }

        // OFFSET clause
        if (offsetValue_ > 0)
        {
            sql << " OFFSET " << offsetValue_;
        }

        return sql.str();
    }

    void QueryBuilder::validateColumn(const std::string& column_) const
    {
        auto it = std::find_if(schema_.begin(), schema_.end(),  [&column_](const ColumnInfo& info) { return info.name == column_; });

        if (it == schema_.end())
        {
            throw std::runtime_error("Column '" + column_ + "' not found in table '" + tableName_ + "'");
        }
    }

    void QueryBuilder::validateColumnType(const std::string& column_, const ColumnValue& value_) const
    {
        // Allow NULL for any column type
        if (std::holds_alternative<std::monostate>(value_))
        {
            return;
        }

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

            throw std::runtime_error( "Type mismatch for column_ '" + column_ + "': " +   "expected " + sqliteType + ", got " + valueType );
        }
    }

    std::string QueryBuilder::getSqliteType(const std::string& column_) const
    {
        auto it = std::find_if(schema_.begin(), schema_.end(),  [&column_](const ColumnInfo& info) { return info.name == column_; });

        if (it != schema_.end())
        {
            return it->type;
        }

        return "";
    }

    bool QueryBuilder::isTypeCompatible(const std::string& sqliteType, const ColumnValue& value_) const
    {
        // SQLite type affinity rules
        // INTEGER affinity
        if (sqliteType.find("INT") != std::string::npos)
        {
            return std::holds_alternative<int64_t>(value_);
        }

        // TEXT affinity
        if (sqliteType.find("CHAR") != std::string::npos ||
            sqliteType.find("CLOB") != std::string::npos ||
            sqliteType.find("TEXT") != std::string::npos)
        {
            return std::holds_alternative<std::string>(value_);
        }

        // BLOB affinity
        if (sqliteType.find("BLOB") != std::string::npos)
        {
            return std::holds_alternative<std::vector<uint8_t>>(value_);
        }

        // REAL affinity
        if (sqliteType.find("REAL") != std::string::npos ||
            sqliteType.find("FLOA") != std::string::npos ||
            sqliteType.find("DOUB") != std::string::npos)
        {
            return std::holds_alternative<double>(value_);
        }

        // NUMERIC affinity - accepts both int and real
        return std::holds_alternative<int64_t>(value_) ||
            std::holds_alternative<double>(value_);
    }

    // ============================================================================
    // QueryFactory Implementation
    // ============================================================================

    QueryFactory::QueryFactory(Analyzer& kdb)
        : analyzer_(kdb)
    {
    }

    QueryBuilder QueryFactory::FromTable(const std::string& tableName)
    {
        return QueryBuilder(analyzer_, tableName);
    }

    InsertBuilder QueryFactory::InsertInto(const std::string& tableName)
    {
        return InsertBuilder(analyzer_, tableName);
    }

    UpdateBuilder QueryFactory::UpdateTable(const std::string& tableName)
    {
        return UpdateBuilder(analyzer_, tableName);
    }

    DeleteBuilder QueryFactory::DeleteFrom(const std::string& tableName)
    {
        return DeleteBuilder(analyzer_, tableName);
    }

} // namespace sqlite_flux