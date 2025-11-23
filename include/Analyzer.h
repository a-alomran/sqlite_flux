// include/Analyzer.h
#pragma once

#include "TableTypes.h"
#include <string>
#include <memory>
#include <optional>

// Forward declaration to avoid including sqlite3.h in header
struct sqlite3;

namespace sqlite_flux
{

	class Analyzer
	{
	public:
		// Constructor with default database path
		explicit Analyzer(const std::string& dbPath);

		// Destructor
		~Analyzer();

		// Disable copy, enable move
		Analyzer(const Analyzer&) = delete;
		Analyzer& operator=(const Analyzer&) = delete;
		Analyzer(Analyzer&&) noexcept;
		Analyzer& operator=(Analyzer&&) noexcept;

		// Open/close database
		bool open(const std::string& dbPath);
		void close();
		bool isOpen() const;

		// Table discovery
		std::vector<std::string> getTableNames() const;
		std::vector<std::string> getColumnNames(const std::string& tableName) const;
		TableSchema getTableSchema(const std::string& tableName) const;

		// Query operations (thread-safe)
		ResultSet query(const std::string& sql) const;
		ResultSet selectAll(const std::string& tableName) const;
		ResultSet selectWhere(const std::string& tableName,
			const std::string& whereClause) const;

		// Execute operations (INSERT, UPDATE, DELETE) - thread-safe
		bool execute(const std::string& sql);

		// Transaction support - thread-safe
		bool beginTransaction();
		bool commit();
		bool rollback();

		// Get last error message
		std::string getLastError() const;

		// Get row count for a table
		std::optional<int64_t> getRowCount(const std::string& tableName) const;

		// Schema caching - thread-safe
		void cacheAllSchemas();
		bool isSchemaCached() const;
		void clearSchemaCache();
		std::optional<TableSchema> getCachedSchema(const std::string& tableName) const;

		// WAL mode configuration for web applications
		bool enableWALMode();
		bool isWALMode() const;

	private:
		struct Impl;
		std::unique_ptr<Impl> pImpl_;
	};

} // namespace sqlite_flux