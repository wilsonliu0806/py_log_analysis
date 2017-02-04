import sqlalchemy
from sqlalchemy import create_engine
from bokeh.charts import Histogram,output_file,show
from bokeh.plotting import curdoc,figure
from bokeh.models.widgets import Button,Slider
from bokeh.layouts import column,row,widgetbox
import pandas as pd

engine = create_engine('mysql+mysqldb://root:root@localhost/squidlog')
db_conn = engine.connect()
sql = "select url,cur_file_sz from access_status where hit = 'TCP_HSD_HIT'"
df = pd.read_sql(sql,db_conn)

#slider
sliderL1 = Slider(start=0,end=10240,value=1024,step=1024,title="L1")
sliderL2 = Slider(start=10240,end=10*1024*1024,step=1024*1024,title="L2")
button_refresh = Button(label="Fresh")
widget_box = widgetbox(sliderL1,sliderL2,button_refresh)
#figure times 
b1 = int()
b2 = int()
b3 = int()
l1 = 1024
l2 = 5*1024*1024
p = figure(width=400,height=400)
pvbar=p.vbar (x=[1,2,3],width=0.5,bottom=0,top=[b1,b2,b3],color="firebrick")
p2 = figure(width=400)
pvbar2=p2.vbar(x=[1,2,3],width=0.5,bottom=0,top=[b1,b2,b3],color="firebrick")


def get_times():
    global b1,b2,b3
    b1=b2=b3=0
    for col in df['cur_file_sz']:
        if col < l1:
            b1+=1
        if col > l2:
            b3+=1
        else:
            b2+=1
def get_size():
    #figure size 
    global b1,b2,b3
    b1=b2=b3=0
    for col in df['cur_file_sz']:
        if col < l1:
            b1+= col
        if col > l2:
            b3+= col
        else:
            b2+= col
#output_file("Histogram_color.html")
def update():
    l1 = sliderL1.value
    l2 = sliderL2.value
    get_times()
    global b1,b2,b3
    pvbar.data_source.data["top"]=[b1,b2,b3]
    get_size()
    pvbar2.data_source.data["top"]=[b1,b2,b3]

button_refresh.on_click(update)
update()
curdoc().add_root(row(widget_box,p,p2))
