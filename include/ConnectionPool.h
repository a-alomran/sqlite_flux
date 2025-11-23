// include/ConnectionPool.h
#pragma once

#include "Analyzer.h"
#include <queue>
#include <mutex>
#include <condition_variable>
#include <memory>
#include <chrono>
#include <optional>
#include <atomic>

namespace sqlite_flux
{

	class ConnectionPool
	{
	public:
		// RAII connection guard - automatically returns connection to pool
		class Connection
		{
		public:
			Connection(ConnectionPool* pool, std::unique_ptr<Analyzer> conn);
			~Connection();

			// Disable copy, enable move
			Connection(const Connection&) = delete;
			Connection& operator=(const Connection&) = delete;
			Connection(Connection&&) noexcept;
			Connection& operator=(Connection&&) noexcept;

			// Access the underlying Analyzer
			Analyzer* operator->() { return conn_.get(); }
			Analyzer& operator*() { return *conn_; }
			const Analyzer* operator->() const { return conn_.get(); }
			const Analyzer& operator*() const { return *conn_; }

			// Check if connection is valid
			bool isValid() const { return conn_ != nullptr; }

		private:
			ConnectionPool* pool_;
			std::unique_ptr<Analyzer> conn_;
		}; // end of class Connection

		// Constructor
		explicit ConnectionPool(
			const std::string& dbPath,
			size_t poolSize = 10,
			bool enableWAL = true);

		// Destructor
		~ConnectionPool();

		// Disable copy and move
		ConnectionPool(const ConnectionPool&) = delete;
		ConnectionPool& operator=(const ConnectionPool&) = delete;
		ConnectionPool(ConnectionPool&&) = delete;
		ConnectionPool& operator=(ConnectionPool&&) = delete;

		// Acquire a connection from the pool (blocks if none available)
		Connection acquire();

		// Try to acquire a connection with timeout
		std::optional<Connection> tryAcquire(std::chrono::milliseconds timeout);

		// Get pool statistics
		size_t size() const;
		size_t available() const;
		size_t inUse() const;
		size_t outstandingConnections() const;

	private:
		// Release connection back to pool
		void release(std::unique_ptr<Analyzer> conn);

		std::string dbPath_;
		size_t poolSize_;
		bool enableWAL_;

		std::queue<std::unique_ptr<Analyzer>> pool_;
		mutable std::mutex mutex_;
		std::condition_variable cv_;

		size_t totalConnections_;
		std::atomic<bool> shutdown_{ false };
		std::atomic<size_t> outstandingConnections_{ 0 };
	}; // end of class ConnectionPool

} // namespace sqlite_flux