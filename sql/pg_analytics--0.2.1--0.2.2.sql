\echo Use "ALTER EXTENSION pg_analytics UPDATE TO '0.2.2'" to load this file. \quit

DROP FUNCTION IF EXISTS time_bucket(_bucket_width pg_catalog."interval", _input date);
DROP FUNCTION IF EXISTS time_bucket(_bucket_width pg_catalog."interval", _input date, _offset pg_catalog."interval");
DROP FUNCTION IF EXISTS time_bucket(_bucket_width pg_catalog."interval", _input date, _origin date);
DROP FUNCTION IF EXISTS time_bucket(_bucket_width pg_catalog."interval", _input pg_catalog."timestamp");
DROP FUNCTION IF EXISTS time_bucket(_bucket_width pg_catalog."interval", _input pg_catalog."timestamp", _origin date);
DROP FUNCTION IF EXISTS time_bucket(_bucket_width pg_catalog."interval", _input pg_catalog."timestamp", _offset pg_catalog."interval");
