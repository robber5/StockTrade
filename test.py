import pandas as pd
import matplotlib.pyplot as plt

df = pd.read_csv('fundvalue.csv')
df['diff'] = df['fundvalue']/df['benchmarkvalue']
df = df.loc[:, ['date', 'diff']]
df.to_csv('fundvaluediff.csv', encoding='gbk')
plt.plotfile('fundvaluediff.csv', ('date', 'diff'), subplots=False)
plt.show()

