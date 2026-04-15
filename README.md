### New Learned Skills:

- when pandas make enclousers with "" when the values have commas in them, it is to prevent the commas from being misinterpreted as delimiters when reading the CSV file. This is a common practice in CSV formatting to ensure that the data is correctly parsed, especially when values contain special characters like commas.
  **so when put it into snowflake we should make the file format csv with the option of field_optionally_enclosed_by = '"'**
  '''sql
  CREATE OR ALTER FILE FORMAT csv_format
  TYPE = CSV
  FIELD_DELIMITER = ","
  SKIP_HEADER = 1
  FIELD_OPTIONALLY_ENCLOSED_BY = '"';
  '''
