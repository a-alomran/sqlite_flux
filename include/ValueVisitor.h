// include/ValueVisitor.hpp
#pragma once

#include "ColumnValue.h"
#include <optional>
#include <iostream>
#include <variant> 
#include "TableTypes.h"

namespace sqlite_flux 
{

	// Generic visitor helper (C++20 style)
	template<class... Ts>
	struct overloaded : Ts... 
	{
		using Ts::operator()...;
	};

	template<class... Ts>
	overloaded(Ts...) -> overloaded<Ts...>;

	// Utility functions for working with ColumnValue
	void printValue(const ColumnValue& value);

	// Type-safe value extraction
	template<typename T>
	std::optional<T> getValue(const Row& row, const std::string& key) 
	{
		auto it = row.find(key);
		if (it == row.end()) {
			return std::nullopt;
		}

		if (auto* val = std::get_if<T>(&it->second)) 
		{
			return *val;
		}
		return std::nullopt;
	}

	// Check if value is null
	inline bool isNull(const ColumnValue& value) 
	{
		return std::holds_alternative<std::monostate>(value);
	}

} // namespace sqlite_flux