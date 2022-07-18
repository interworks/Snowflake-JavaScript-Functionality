-- SP_CONDITIONAL_VIEW_CREATION

-- Stored procedure to create a view of a given table, conditionally verifying the existance of the source table first.

-- Each code section is split by a line of -----------------
-- To execute the code, ensure all code within a pair of ----------------- is executed together (most important for the SProc definition statement)

----------------------------

USE ROLE CONSULTANT;
USE DATABASE CHRIS_HASTIE_DB;
USE SCHEMA SPROC_EXAMPLES;
USE WAREHOUSE LOAD_WH;

----------------------------

CREATE OR REPLACE PROCEDURE SP_CONDITIONAL_VIEW_CREATION(
	  ORIGIN_DATABASE varchar
	, ORIGIN_SCHEMA varchar
	, ORIGIN_TABLE varchar
	, DESTINATION_DATABASE varchar
	, DESTINATION_SCHEMA varchar
	, DESTINATION_VIEW varchar
	)
  RETURNS string not null
  language javascript
  execute as caller
as
  $$
  
	let RESULT = "Default result if no other is provided";

	// Define the SQL command to verify the existence of the origin table
	let existenceSqlStatement = snowflake.createStatement( {sqlText: `
		SELECT COUNT(*) FROM "${ORIGIN_DATABASE}".INFORMATION_SCHEMA.TABLES
		WHERE TABLE_SCHEMA = '${ORIGIN_SCHEMA}'
			AND TABLE_NAME = '${ORIGIN_TABLE}'
	;`
	} );

	// Attempt to execute the SQL command to verify the existence of the origin table
	// If the table exists, the destination view will be created
	try {
		var existenceSqlResult = existenceSqlStatement.execute();

		while (existenceSqlResult.next()) {

			// Read the result of the previous query (i.e. the count(*)) into a JavaScript variable
			let EXISTENCE_FLAG = existenceSqlResult.getColumnValue(1);

			// If (EXISTENCE_FLAG == 1) is equivalent to saying if the record count of tables from the information schema is equal to 1
			// This will only be true of the table exists 
			if (EXISTENCE_FLAG == 1) {
				// Define the SQL command to create the view
				let executeSqlStatement = snowflake.createStatement( {sqlText: `
					CREATE OR REPLACE VIEW "${DESTINATION_DATABASE}"."${DESTINATION_SCHEMA}"."${DESTINATION_VIEW}"
						COPY GRANTS
					AS
						SELECT * FROM "${ORIGIN_DATABASE}"."${ORIGIN_SCHEMA}"."${ORIGIN_TABLE}"
				;`}  );

				// Attempt to execute the SQL command to create the view
				try {
					var executeSqlResultSet = executeSqlStatement.execute();
					RESULT = `SUCCESS - View created: "${DESTINATION_DATABASE}"."${DESTINATION_SCHEMA}"."${DESTINATION_VIEW}"`;
					return RESULT;
				}

				catch(err) {
					RESULT = `FAIL - Error creating view: "${ORIGIN_DATABASE}"."${ORIGIN_SCHEMA}"."${ORIGIN_TABLE}"`
					RESULT += `\nError: \n ${err}`;
					return RESULT;
				}
			}
			else {
				RESULT = `FAIL - Given table does not exist: "${ORIGIN_DATABASE}"."${ORIGIN_SCHEMA}"."${ORIGIN_TABLE}"`
				return RESULT;
			}
		}
	}

	catch(err) {
		RESULT = `FAIL - Error verifying table existence: "${ORIGIN_DATABASE}"."${ORIGIN_SCHEMA}"."${ORIGIN_TABLE}"`
		RESULT += `\nError: \n ${err}`;
		return RESULT;
	}

    return RESULT;

  $$
;



------------------------------------------------------------------

-- Unit test
/*

CALL SP_CONDITIONAL_VIEW_CREATION('CHRIS_HASTIE_DB', 'SPROC_EXAMPLES', 'INPUT_TABLE', 'CHRIS_HASTIE_DB', 'SPROC_EXAMPLES', 'MY_VIEW');

*/
