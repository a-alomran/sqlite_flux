// include/ColumnValue.hpp
#pragma once
#include <variant>
#include <string>
#include <vector>
#include <cstdint>

namespace sqlite_flux 
{
	// Define a variant type that can hold all possible SQLite column types
	using ColumnValue = std::variant<std::monostate,// NULL
		int64_t,                 // INTEGER
		double,                  // REAL
		std::string,             // TEXT
		std::vector<uint8_t> > ;// BLOB

} // namespace sqlite_flux