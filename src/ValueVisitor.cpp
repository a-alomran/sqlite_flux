#include <variant> 
#include "ValueVisitor.h"
#include <iostream>

namespace sqlite_flux {

	void printValue(const ColumnValue& value) {
		std::visit(overloaded{
			[](std::monostate) { std::cout << "NULL"; },
			[](int64_t v) { std::cout << v; },
			[](double v) { std::cout << v; },
			[](const std::string& v) { std::cout << '"' << v << '"'; },
			[](const std::vector<uint8_t>& v) {
				std::cout << "[BLOB: " << v.size() << " bytes]";
			}
			}, value);
	}

} // namespace sqlite_flux