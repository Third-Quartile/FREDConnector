
import logging
import azure.functions as func

from fred_etl import fred_main

app = func.FunctionApp()

@app.schedule(schedule="0 0 10 * * *", arg_name="myTimer", run_on_startup=False,
              use_monitor=False) 
def fred_timer_trigger(myTimer: func.TimerRequest) -> None:
    if myTimer.past_due:
        logging.info('The timer is past due!')

    fred_main()

    logging.info('Python timer trigger function executed.')