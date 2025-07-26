import azure.functions as func
import json
import logging
import snowflake.connector
import time
import os

app = func.FunctionApp(http_auth_level=func.AuthLevel.FUNCTION)

@app.route(route="GetSP")
def GetSP(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Azure Function triggered.')

    try:
        req_body = req.get_json()
    except ValueError:
        req_body = {}

    
    if isinstance(req_body, list) and len(req_body) > 0:
        first_event = req_body[0]
        if first_event.get('eventType') == 'Microsoft.EventGrid.SubscriptionValidationEvent':
            validation_code = first_event['data']['validationCode']
            logging.info(f"Validating Event Grid subscription with code: {validation_code}")
            return func.HttpResponse(
                body=json.dumps({'validationResponse': validation_code}),
                status_code=200,
                mimetype='application/json'
            )

    
    logging.info("Delay for ingestion to complete")
    time.sleep(20)

   
    try:
        ctx = snowflake.connector.connect(
            user=os.environ["SNOWFLAKE_USER"],
            password=os.environ["SNOWFLAKE_PASSWORD"],
            account=os.environ["SNOWFLAKE_ACCOUNT"],
            warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
            database=os.environ["SNOWFLAKE_DATABASE"],
            schema=os.environ["SNOWFLAKE_SCHEMA"],
            role=os.environ["SNOWFLAKE_ROLE"]
        )
        cs = ctx.cursor()
        try:
            cs.execute("CALL TRANSFORM_RAW_PRODUCTS_FULL();")
            result = cs.fetchall()
            logging.info(f"Stored procedure executed, result: {result}")
            return func.HttpResponse(
                "Snowflake SP executed successfully after Event Grid trigger and delay.",
                status_code=200
            )
        finally:
            cs.close()
            ctx.close()

    except Exception as e:
        logging.error(str(e))
        return func.HttpResponse(
            f"Error executing Snowflake SP: {str(e)}",
            status_code=500
        )
