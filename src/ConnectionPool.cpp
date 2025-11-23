// src/ConnectionPool.cpp
#include "ConnectionPool.h"
#include <stdexcept>
#include <cassert>

namespace sqlite_flux
{

	// ============================================================================
	// Connection implementation
	// ============================================================================

	ConnectionPool::Connection::Connection(ConnectionPool* pool, std::unique_ptr<Analyzer> conn)
		: pool_(pool), conn_(std::move(conn))
	{
	} // end of Connection constructor

	ConnectionPool::Connection::~Connection()
	{
		if (conn_ && pool_)
		{
			pool_->release(std::move(conn_));
		} // end of if
	} // end of Connection destructor

	ConnectionPool::Connection::Connection(Connection&& other) noexcept
		: pool_(other.pool_), conn_(std::move(other.conn_))
	{
		other.pool_ = nullptr;
	} // end of Connection move constructor

	ConnectionPool::Connection& ConnectionPool::Connection::operator=(Connection&& other) noexcept
	{
		if (this != &other)
		{
			// Release current connection first
			if (conn_ && pool_)
			{
				pool_->release(std::move(conn_));
			} // end of if

			pool_ = other.pool_;
			conn_ = std::move(other.conn_);
			other.pool_ = nullptr;
		} // end of if

		return *this;
	} // end of Connection move assignment

	// ============================================================================
	// ConnectionPool implementation
	// ============================================================================

	ConnectionPool::ConnectionPool(
		const std::string& dbPath,
		size_t poolSize,
		bool enableWAL)
		: dbPath_(dbPath)
		, poolSize_(poolSize)
		, enableWAL_(enableWAL)
		, totalConnections_(0)
	{
		if (poolSize == 0)
		{
			throw std::invalid_argument("Connection pool size must be greater than 0");
		} // end of if

		// Pre-create all connections
		for (size_t i = 0; i < poolSize_; ++i)
		{
			auto conn = std::make_unique<Analyzer>(dbPath_);

			if (!conn->isOpen())
			{
				throw std::runtime_error("Failed to open database connection: " + conn->getLastError());
			} // end of if

			// Enable WAL mode if requested
			if (enableWAL_)
			{
				conn->enableWALMode();
			} // end of if

			// Cache schemas once per connection (optimization)
			conn->cacheAllSchemas();

			pool_.push(std::move(conn));
			++totalConnections_;
		} // end of for
	} // end of ConnectionPool constructor

	ConnectionPool::~ConnectionPool()
	{
		shutdown_.store(true, std::memory_order_release);
		cv_.notify_all();

		// Check for outstanding connections (safety check)
		size_t outstanding = outstandingConnections_.load(std::memory_order_acquire);
		if (outstanding > 0)
		{
			// In debug builds, assert to catch misuse early
			assert(false && "ConnectionPool destroyed with outstanding connections!");

			// In release builds, log warning but don't crash
			// Note: This is still undefined behavior if connections are used after pool destruction
		} // end of if
	} // end of ConnectionPool destructor

	ConnectionPool::Connection ConnectionPool::acquire()
	{
		std::unique_lock lock(mutex_);

		// Wait until a connection is available
		cv_.wait(lock, [this] {
			return !pool_.empty() || shutdown_.load(std::memory_order_acquire);
			});

		if (shutdown_.load(std::memory_order_acquire))
		{
			throw std::runtime_error("Connection pool is shutting down");
		} // end of if

		if (pool_.empty())
		{
			throw std::runtime_error("No connections available (should not happen)");
		} // end of if

		auto conn = std::move(pool_.front());
		pool_.pop();

		outstandingConnections_.fetch_add(1, std::memory_order_relaxed);

		return Connection(this, std::move(conn));
	} // end of acquire

	std::optional<ConnectionPool::Connection> ConnectionPool::tryAcquire(std::chrono::milliseconds timeout)
	{
		std::unique_lock lock(mutex_);

		// Wait with timeout
		bool available = cv_.wait_for(lock, timeout, [this] {
			return !pool_.empty() || shutdown_.load(std::memory_order_acquire);
			});

		if (shutdown_.load(std::memory_order_acquire))
		{
			throw std::runtime_error("Connection pool is shutting down");
		} // end of if

		if (!available || pool_.empty())
		{
			return std::nullopt;  // Timeout or no connections
		} // end of if

		auto conn = std::move(pool_.front());
		pool_.pop();

		outstandingConnections_.fetch_add(1, std::memory_order_relaxed);

		return Connection(this, std::move(conn));
	} // end of tryAcquire

	void ConnectionPool::release(std::unique_ptr<Analyzer> conn)
	{
		if (!conn)
		{
			return;  // Nothing to release
		} // end of if

		outstandingConnections_.fetch_sub(1, std::memory_order_relaxed);

		std::lock_guard lock(mutex_);
		pool_.push(std::move(conn));
		cv_.notify_one();  // Notify waiting threads
	} // end of release

	size_t ConnectionPool::size() const
	{
		return totalConnections_;
	} // end of size

	size_t ConnectionPool::available() const
	{
		std::lock_guard lock(mutex_);
		return pool_.size();
	} // end of available

	size_t ConnectionPool::inUse() const
	{
		std::lock_guard lock(mutex_);
		return totalConnections_ - pool_.size();
	} // end of inUse

	size_t ConnectionPool::outstandingConnections() const
	{
		return outstandingConnections_.load(std::memory_order_relaxed);
	} // end of outstandingConnections

} // namespace sqlite_flux