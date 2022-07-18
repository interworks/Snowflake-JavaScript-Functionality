
-- Execute a metadata command and drop the results into a table

------------------------------------------------------------------
-- Create the stored procedure

CREATE OR REPLACE PROCEDURE SP_METADATA_COMMAND_TO_TABLE(
    METADATA_COMMAND varchar
  , DESTINATION_TABLE varchar
)
  returns string not null
  language javascript
  execute as caller -- Must execute as caller to support the SHOW command
as
  $$
  
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    //// Setup
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    // Define RESULT variable that will be output at the end,
    // or output if an caught error occurs
    var RESULT = "";
  
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    //// Validate inputs 
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    /////////////////////////////////////////////////////////////////////////
    // Define function to confirm that a variable is populated

    function validate_variable_populated(VARIABLE_NAME, VARIABLE_VALUE) {
      // This function ensures that the provided variable is not blank/null
      let FUNCTION_RESULT = '';
      let FUNCTION_RESULT_FLAG = false;

      if (VARIABLE_VALUE) {
        if (!(VARIABLE_VALUE.length > 0)) {
          FUNCTION_RESULT_FLAG = true;
          FUNCTION_RESULT = `Failed: ${VARIABLE_NAME} parameter must be populated`;
        };
      } else {
        FUNCTION_RESULT_FLAG = true;
        FUNCTION_RESULT = `Failed: ${VARIABLE_NAME} parameter must be populated`;
      };

      return [FUNCTION_RESULT, FUNCTION_RESULT_FLAG];
    };

    /////////////////////////////////////////////////////////////////////////
    // Error if any variables are not provided

    REQUIRED_VARIABLES_LIST = [
        {
            "VARIABLE_NAME": 'METADATA_COMMAND'
          , "VARIABLE_VALUE": METADATA_COMMAND
        }
      , {
            "VARIABLE_NAME": 'DESTINATION_TABLE'
          , "VARIABLE_VALUE": DESTINATION_TABLE
        }
    ];

    // Loop through the list of variables defined above and execute the validate_variable_populated function
    // for each one, returning an error that ends the procedure early if any variables are found to be blank/null
    REQUIRED_VARIABLES_LIST.forEach(function(REQUIRED_VARIABLE) {
      let FUNCTION_OUTPUT = validate_variable_populated(REQUIRED_VARIABLE.VARIABLE_NAME, REQUIRED_VARIABLE.VARIABLE_VALUE);
      RESULT += FUNCTION_OUTPUT[0];
      if (FUNCTION_OUTPUT[1] === false) {
        return RESULT; 
      };
    });
  
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    //// Define and execute SQL commands 
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    
    ////////////////////////////////////////////////////
    // Execute the metadata command

    // This process also captures the query ID of the metadata command so that it can be retrieved after

    var SQL_METADATA_COMMAND = snowflake.createStatement( {sqlText: METADATA_COMMAND});
    var SQL_QUERY_ID;

    try {
      SQL_METADATA_COMMAND.execute();
      SQL_QUERY_ID = SQL_METADATA_COMMAND.getQueryId();
      RESULT += `Succeeded: Metadata command executed`;
      RESULT += "\nQuery ID: " + SQL_QUERY_ID;
    }

    catch (err) {
      RESULT += `Failed: Error executing metadata`;
      RESULT += "\nSQL: " + SQL_METADATA_COMMAND.getSqlText();
      RESULT += "\nMessage: " + err.message;
	    return RESULT;
    }
    
    ////////////////////////////////////////////////////
    // Insert the results of the metadata command into the destination table
    
    var SQL_INSERT_RESULTS_INTO_TABLE_COMMAND_TEXT = `
      CREATE OR REPLACE TABLE ${DESTINATION_TABLE}
      AS
      SELECT *
      FROM TABLE(RESULT_SCAN('${SQL_QUERY_ID}'))
      ;
    `

    var SQL_INSERT_RESULTS_INTO_TABLE_COMMAND = snowflake.createStatement( {sqlText: SQL_INSERT_RESULTS_INTO_TABLE_COMMAND_TEXT});

    try {
      SQL_INSERT_RESULTS_INTO_TABLE_COMMAND.execute();
      RESULT += `\nSucceeded: Results inserted into table ${DESTINATION_TABLE}`;
    }

    catch (err) {
      RESULT += `\nFailed: Error inserting results into destination table`;
      RESULT += "\nSQL: " + SQL_INSERT_RESULTS_INTO_TABLE_COMMAND.getSqlText();
      RESULT += "\nMessage: " + err.message;
	    return RESULT;
    }

    return RESULT;
  $$
;

/*
------------------------------------------------------------------
-- Testing

CALL SP_METADATA_COMMAND_TO_TABLE(
    'SHOW DATABASES'              -- METADATA_COMMAND varchar
  , 'MY_TEST_TABLE'               -- DESTINATION_TABLE varchar
)
;

SELECT * FROM MY_TEST_TABLE;

*/
