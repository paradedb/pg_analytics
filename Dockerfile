# Stage 1: Builder
FROM rust:1.84 as builder

# Install necessary system packages
RUN apt-get update && apt-get install -y \
    git \
    libpq-dev \
    pkg-config \
    build-essential \
    postgresql-server-dev-all \
    && rm -rf /var/lib/apt/lists/*

# Set environment variables for Cargo
ENV CARGO_HOME=/usr/local/cargo \
    RUSTUP_HOME=/usr/local/rustup \
    PATH=/usr/local/cargo/bin:$PATH

# Install cargo pgrx v0.12.7
RUN cargo install cargo-pgrx --version 0.12.7

# Verify cargo pgrx installation
RUN cargo pgrx --version

# Clone the Git repository containing the pgrx extension
RUN git clone https://github.com/paradedb/pg_analytics /usr/src/pg_analytics/

# Set the working directory
WORKDIR /usr/src/pg_analytics/

# Checkout the specific branch
RUN git checkout feat/load-httpfs

# Build the pgrx extension using cargo pgrx
RUN cargo pgrx build --release

# Stage 2: Runtime
FROM debian:bullseye-slim

# Install runtime dependencies and PostgreSQL
RUN apt-get update && apt-get install -y \
    libpq5 \
    postgresql \
    && rm -rf /var/lib/apt/lists/*

# Create directories for PostgreSQL data and configuration
RUN mkdir -p /var/lib/postgresql/data && \
    mkdir -p /var/log/postgresql && \
    mkdir -p /var/run/postgresql && \
    mkdir -p /usr/share/postgresql/extension

# Declare volumes for writable directories
VOLUME ["/var/lib/postgresql/data", "/var/log/postgresql", "/var/run/postgresql", "/usr/share/postgresql/extension"]

# Copy the built extension from the builder stage
COPY --from=builder /usr/src/pg_analytics/target/release/pg_analytics-pg17/usr/lib/postgresql/17/lib/* /usr/lib/postgresql/17/lib/
COPY --from=builder /usr/src/pg_analytics/target/release/pg_analytics-pg17/usr/share/postgresql/17/extension/* /usr/share/postgresql/17/extension/

# Set appropriate permissions
RUN chown -R postgres:postgres /var/lib/postgresql/data /var/log/postgresql /var/run/postgresql /usr/share/postgresql/extension

# Switch to the postgres user
USER postgres

# Initialize PostgreSQL data directory if not already initialized
RUN /usr/lib/postgresql/17/bin/initdb -D /var/lib/postgresql/data

# Expose PostgreSQL port
EXPOSE 5432

# Set default command to run PostgreSQL
CMD ["postgres", "-D", "/var/lib/postgresql/data", "-c", "config_file=/etc/postgresql/17/main/postgresql.conf"]
