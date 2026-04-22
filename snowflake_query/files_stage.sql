CREATE
OR ALTER file format csv_format TYPE = csv field_delimiter = "," skip_header = 1 field_optionally_enclosed_by = '"';
CREATE stage my_int_stage file_format = csv_format;
