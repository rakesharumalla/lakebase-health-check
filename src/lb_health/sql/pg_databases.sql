SELECT datname FROM pg_database WHERE NOT datistemplate AND datallowconn ORDER BY datname;
