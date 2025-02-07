-- src/api/parquet.rs:52
-- pg_analytics::api::parquet::parquet_describe
DROP FUNCTION "parquet_describe"(TEXT);
CREATE OR REPLACE FUNCTION "parquet_describe"(
	"relation" regclass /* pgrx::rel::PgRelation */
) RETURNS TABLE (
	"column_name" TEXT,  /* core::option::Option<alloc::string::String> */
	"column_type" TEXT,  /* core::option::Option<alloc::string::String> */
	"null" TEXT,  /* core::option::Option<alloc::string::String> */
	"key" TEXT,  /* core::option::Option<alloc::string::String> */
	"default" TEXT,  /* core::option::Option<alloc::string::String> */
	"extra" TEXT  /* core::option::Option<alloc::string::String> */
)
STRICT
LANGUAGE c /* Rust */
AS 'MODULE_PATHNAME', 'parquet_describe_wrapper';
/* </end connected objects> */

-- src/api/parquet.rs:73
-- pg_analytics::api::parquet::parquet_schema
DROP FUNCTION "parquet_schema"(TEXT);
CREATE OR REPLACE FUNCTION "parquet_schema"(
	"relation" regclass /* pgrx::rel::PgRelation */
) RETURNS TABLE (
	"file_name" TEXT,  /* core::option::Option<alloc::string::String> */
	"name" TEXT,  /* core::option::Option<alloc::string::String> */
	"type" TEXT,  /* core::option::Option<alloc::string::String> */
	"type_length" TEXT,  /* core::option::Option<alloc::string::String> */
	"repetition_type" TEXT,  /* core::option::Option<alloc::string::String> */
	"num_children" bigint,  /* core::option::Option<i64> */
	"converted_type" TEXT,  /* core::option::Option<alloc::string::String> */
	"scale" bigint,  /* core::option::Option<i64> */
	"precision" bigint,  /* core::option::Option<i64> */
	"field_id" bigint,  /* core::option::Option<i64> */
	"logical_type" TEXT  /* core::option::Option<alloc::string::String> */
)
STRICT
LANGUAGE c /* Rust */
AS 'MODULE_PATHNAME', 'parquet_schema_wrapper';
/* </end connected objects> */
