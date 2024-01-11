import numpy as np
import pandas as pd
from scipy.optimize import minimize
from statsmodels.formula.api import ols

def OU_parameters_estimation(data):
    # Objective function for maximum likelihood estimation
    def loss(params):
        mu, theta, sigma = params
        n = len(data)
        dt = 1  # assuming uniform spacing
        residuals = np.diff(data) - theta * (mu - data[:-1]) * dt
        loglikelihood = -np.sum(residuals ** 2) / (2 * sigma ** 2) - 0.5 * n * np.log(2 * np.pi * sigma ** 2)
        return -loglikelihood  # we want to maximize loglikelihood

    # Gradient of the objective function
    def gradient(params):
        mu, theta, sigma = params
        n = len(data)
        dt = 1  # assuming uniform spacing
        residuals = np.diff(data) - theta * (mu - data[:-1]) * dt

        dmu = np.sum(residuals) / sigma ** 2
        dtheta = np.sum((mu - data[:-1]) * residuals) / sigma ** 2
        dsigma = -n / sigma + np.sum(residuals ** 2) / sigma ** 3

        return -np.array([dmu, dtheta, dsigma])

    # Initial guess
    initial_guess = [np.mean(data), abs(0.1), abs(np.std(data))]

    # Setting the bounds
    no_bound = (-np.inf, np.inf)
    positive_bound = (1e-10, np.inf)  # Using a very small positive number instead of 0 to avoid division by zero or log of zero issues.
    bounds = [no_bound, positive_bound, positive_bound]

    # Optimization
    result = minimize(loss, initial_guess, jac=gradient, bounds=bounds, method='L-BFGS-B')
    mu, theta, sigma = result.x

    # Compute Hessian
    hessian = result.hess_inv.todense()  # Hessian is inverted here
    se_theta = np.sqrt(hessian[1, 1])

    return mu, theta, sigma, se_theta

df = pd.read_csv('uniswap-v3-stationary-swaps2.csv')
mean_reverting_ids = []
bootstrap_ids = []
parameters_df = pd.DataFrame(columns=['pool_contract_address', 'mu', 'theta', 'sigma'])

i = 0
# Loop through each unique ID
for ts_id in df['pool_contract_address'].unique():
    sub_df = df[df['pool_contract_address'] == ts_id].reset_index(drop=True)
    data = sub_df['exchange_rate'].to_numpy()

    mu, theta, sigma, se_theta = OU_parameters_estimation(data)

    # Hypothesis test
    T_statistic = theta / se_theta
    critical_value = 1.96  # For a two-tailed test at 5% significance level

    print(f"Theta: {theta}, Standard Error: {se_theta}, T-statistic: {T_statistic}")
    if np.abs(T_statistic) > critical_value:
        mean_reverting_ids.append(ts_id)
        parameters_df.loc[i] = [ts_id, mu, theta, sigma]
        i += 1
    else:
        print("Theta is not statistically different from 0. The process might not exhibit significant mean reversion.")

mean_reverting_df = df[df['pool_contract_address'].isin(mean_reverting_ids)]

mean_reverting_df.to_csv('uni-v3-mr-swaps2.csv', index=False)

parameters_df.to_csv('uni-v3-mr-parameters2.csv', index=False)