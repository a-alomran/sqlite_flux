// include/TableTypes.hpp
#pragma once

#include "ColumnValue.h"
#include <unordered_map>
#include <vector>
#include <string>

namespace sqlite_flux 
{

	// A row is a map of column names to values
	using Row = std::unordered_map<std::string, ColumnValue>;

	// A result set is a collection of rows
	using ResultSet = std::vector<Row>;

	// Column information
	struct ColumnInfo 
	{
		std::string name;
		std::string type;
		bool notNull;
		bool primaryKey;
	};

	using TableSchema = std::vector<ColumnInfo>;

} // namespace sqlite_flux