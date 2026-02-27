import azure.functions as func
from DailyPlannerParams import bp as dailyplanner_bp
from TimeTickets import bp as timetickets_bp

app = func.FunctionApp()

# Register table blueprints - each table has its own .py file
app.register_functions(dailyplanner_bp)
app.register_functions(timetickets_bp)
