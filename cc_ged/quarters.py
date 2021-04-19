from typing import Tuple
from datetime import date
from dateutils import relativedelta

quarter_of = lambda x: ((x.month-1) // 3) + 1
month_quarter_bounds = lambda x: (((x-1)*3)+1,x*3)

def date_quarter_bounds(year:int,quarter:int)->Tuple[date,date]:
    bounds = month_quarter_bounds(quarter)
    start_date,x = [date(year=year,month=m,day=1) for m in bounds]
    end_date = (x + relativedelta(months=1)) - relativedelta(days=1)
    return start_date,end_date
