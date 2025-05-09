# `[stage03]`

- [`[stage03]`](#stage03)
  - [Data Preprocessing](#data-preprocessing)
    - [Encoding Categorical Features](#encoding-categorical-features)
    - [Working with Datetime](#working-with-datetime)
    - [Numerical Features](#numerical-features)
  - [Modeling](#modeling)
    - [Linear Regression](#linear-regression) 
    - [Random Forest Regression](#random-forest-regression)
    - [Gradient Boosting](#gradient-boosting)
  - [Evaluation](#evaluation) 

This stage is responsible for training the models and performing PDA.

## Data Preprocessing

We split the data into 80% training and 20% testing dataset. We have to split data before preprocessing to avoid data leaking from test set.

Then, we initiate the data preprocessing pipeline.

### Encoding Categorical Features

Some of the features are categorical, so storing them as integer values would make no sense. Those features are:
- Vendor ID
- Ratecode ID
- Payment type
- Trip type

To encode these features, we use One Hot Encoder, and then assemble them into single vector of categorical features using Vector Assembler.

### Working with Datetime

The lpep_pickup_datetime has a datetime value. We need to transform it to numerical features. 

For this, we created a separate DateTimeTransformer class. It obtains the following values from datetime:
- Year of trip
- Month of trip, encoded with sin and cos (cyclical feature)
- Day of the week of trip, encoded with sin and cos (cyclical feature)
- Hour of trip, encoded with sin and cos (cyclical feature)
- Minute of trip, encoded with sin and cos (cyclical feature)
- Second of trip, encoded with sin and cos (cyclical feature)

### Numerical features

To provide more consistent results, numerical features have to be scaled to general range, so that no feature can influence the result only because of big value.
To perform scaling, we simply use StandardScaler.

At the end, all features are concatenated into one Vector.

## Modeling

We have created separate models to predict the fare amount. This is a regression task.

### Linear Regression

Linear Regression is a simple regression models that tries to find coefficients for each of input parameters to estimate a linear function that would predict the resulting value.

We used the following parameter grid for Linear regression:

```python
regParam: [0.01, 0.1, 1]
elasticNetParam: [0.0, 0.5, 1.0]
```

### Random Forest Regression

Random Forest Regression model attempts to find the value of result by creating multiple trees, each of those tries to predict the value based on input variables.

We used the following parameter grid for Random Forest Regression:

```python
numTrees: [25, 50, 75]
maxDepth: [3, 4, 5]
```

### Gradient Boosting

Gradient Boosting attempts to train a few weak learners using Gradient Descent, and combine them into an ansamble.

We used the following parameter grid for Gradient Boosting:

```python
maxDepth: [2, 3, 4, 5, 7]
```

## Evaluation

To evaluate the model, we used:
- Root Mean Square Error (RMSE)
- Determination coefficient (R2)

These metrics are the common ones used for evaluating regression tasks, since RMSE provides general insight on how far prediction is from average value, while R2 shows how well the model captures the determination of results.

