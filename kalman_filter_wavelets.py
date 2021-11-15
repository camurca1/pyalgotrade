# %%
from datetime import datetime
import itertools

import pandas as pd
from pykalman import KalmanFilter
import pywt

import matplotlib.pyplot as plt
import seaborn as sns

# %%
# configurar exibição
pd.set_option('display.max_columns', 200000)
pd.set_option('display.max_rows', 200000)
pd.set_option('display.expand_frame_repr', False)
pd.set_option('use_inf_as_na', True)
sns.set_style('whitegrid')
idx = pd.IndexSlice

# %%
DATA_STORE = 'dados/assets.h5'

# %%
with pd.HDFStore(DATA_STORE) as store:
    ibov = store['ibov'].loc['2010': '2022', 'close']

# %%
# config KF
kf = KalmanFilter(transition_matrices=[1],
                  observation_matrices=[1],
                  initial_state_mean=0,
                  initial_state_covariance=1,
                  observation_covariance=1,
                  transition_covariance=.01)

# %%
# estimate hidden state
state_means, _ = kf.filter(ibov)

# %%
# compare with MA
ibov_smooth = ibov.to_frame('close')
ibov_smooth['Kalman Filter'] = state_means
for months in [1, 2, 3]:
    ibov_smooth[f'MA ({months}m)'] = ibov.rolling(window=months * 21).mean()

# %%
plt.clf()
ax = ibov_smooth.plot(title='Kalman Filter vs Moving Average', figsize=(14, 6), lw=1, rot=0)
ax.set_xlabel('')
ax.set_ylabel('IBOVESPA')
plt.tight_layout()
sns.despine();
plt.savefig('figs/kalmanfilter_ibov')

# %%
# Wavelets
wavelet = pywt.Wavelet('db6')
phi, psi, x = wavelet.wavefun(level=5)
df = pd.DataFrame({'$\phi$': phi, '$\psi$': psi}, index=x)
df.plot(title='Daubechies', subplots=True, layout=(1, 2), figsize=(14, 4), lw=2, rot=0)
plt.tight_layout()
sns.despine();
plt.savefig('figs/wavelets_Daubechies')

# %%
# Smoothing Ibov
signal = (pd.read_hdf(DATA_STORE, 'ibov')
          .loc['2010': '2022']
          .close.pct_change()
          .dropna())

fig, axes = plt.subplots(ncols=2, figsize=(14, 5))

wavelet = "db6"
for i, scale in enumerate([.1, .5]):
    coefficients = pywt.wavedec(signal, wavelet, mode='per')
    coefficients[1:] = [pywt.threshold(i, value=scale * signal.max(), mode='soft') for i in coefficients[1:]]
    reconstructed_signal = pywt.waverec(coefficients, wavelet, mode='per')
    signal.plot(color="b", alpha=0.5, label='original signal', lw=2,
                title=f'Threshold Scale: {scale:.1f}', ax=axes[i])
    pd.Series(reconstructed_signal, index=signal.index).plot(c='k', label='DWT smoothing}', linewidth=1, ax=axes[i])
    axes[i].legend()
fig.tight_layout()
sns.despine();
plt.savefig('figs/smoothed_ibov')