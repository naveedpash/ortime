# coding: utf-8
exec(open("ortime.py").read())
import plotly.express as px
px.histogram(times, x="PREP").show()
get_ipython().run_line_magic('save', '')
get_ipython().run_line_magic('save', 'session.py')
get_ipython().run_line_magic('save', 'session')
get_ipython().run_line_magic('pinfo', '%save')
get_ipython().run_line_magic('save', 'session n1-8')
