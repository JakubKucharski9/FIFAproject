import sqlite3
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score, classification_report, confusion_matrix
from sklearn.ensemble import GradientBoostingClassifier
from xgboost import XGBClassifier
from lightgbm import LGBMClassifier
from sklearn.neural_network import MLPClassifier
import matplotlib.pyplot as plt
import seaborn as sns


# Load data
def load_data():
    with sqlite3.connect('database.sqlite') as conn:
        query = "SELECT * FROM Match_for_plots"
        data = pd.read_sql_query(query, conn)
        data = data.dropna()
        return data


# Prepare data
def prepare_data(data):
    home_features = [col for col in data.columns if col.startswith('home_avg_')]
    away_features = [col for col in data.columns if col.startswith('away_avg_')]

    for home_col, away_col in zip(home_features, away_features):
        feature_name = home_col.replace('home_avg_', '')
        diff_col_name = f'diff_{feature_name}'
        data[diff_col_name] = data[home_col] - data[away_col]
    diff_features = [col for col in data.columns if col.startswith('diff_')]
    X = data[diff_features]
    y = data['prediction']

    # Remove draws
    data = data[data['prediction'] != 0]

    # Ensure balanced classes
    class_counts = data['prediction'].value_counts()
    print(f"Class counts before balancing: \n{class_counts}")

    if class_counts.min() == 0:
        raise ValueError("One of the classes has no samples after removing draws.")

    min_class_count = class_counts.min()
    balanced_data = pd.concat([
        data[data['prediction'] == label].sample(n=min_class_count, random_state=42)
        for label in class_counts.index
    ])
    print(f"Class counts after balancing: \n{balanced_data['prediction'].value_counts()}")

    X_balanced = balanced_data[diff_features]
    y_balanced = balanced_data['prediction']

    # Map classes to range [0, N-1] for compatibility with certain models
    y_balanced = y_balanced.map({1: 0, 2: 1})

    # Train-test split
    return train_test_split(X_balanced, y_balanced, test_size=0.2, random_state=42, stratify=y_balanced)


# Train and evaluate a model
def train_and_evaluate_model(model, X_train, X_test, y_train, y_test, model_name):
    # Validate input shapes
    print(f"X_train shape: {X_train.shape}, X_test shape: {X_test.shape}")
    print(f"y_train shape: {y_train.shape}, y_test shape: {y_test.shape}")

    print(f"\nEvaluating {model_name}...")
    model.fit(X_train, y_train)
    y_pred = model.predict(X_test)

    accuracy = accuracy_score(y_test, y_pred)
    print(f"Accuracy: {accuracy:.2f}")
    print(classification_report(y_test, y_pred))

    # Confusion matrix
    conf_matrix = confusion_matrix(y_test, y_pred)
    plt.figure(figsize=(6, 5))
    sns.heatmap(conf_matrix, annot=True, fmt='d', cmap='Blues',
                xticklabels=["Home Win", "Away Win"],
                yticklabels=["Home Win", "Away Win"])
    plt.title(f"Confusion Matrix - {model_name}")
    plt.xlabel("Predicted")
    plt.ylabel("Actual")
    plt.show()


# Main function
if __name__ == '__main__':
    data = load_data()
    X_train, X_test, y_train, y_test = prepare_data(data)

    # Random Forest (baseline)
    from sklearn.ensemble import RandomForestClassifier

    rf_model = RandomForestClassifier(n_estimators=200, max_depth=15, random_state=42, class_weight='balanced')
    train_and_evaluate_model(rf_model, X_train, X_test, y_train, y_test, "Random Forest")

    # Gradient Boosting
    gb_model = GradientBoostingClassifier(n_estimators=200, max_depth=5, random_state=42)
    train_and_evaluate_model(gb_model, X_train, X_test, y_train, y_test, "Gradient Boosting")

    # XGBoost
    xgb_model = XGBClassifier(n_estimators=200, max_depth=5, random_state=42, use_label_encoder=False,
                              eval_metric='mlogloss')
    train_and_evaluate_model(xgb_model, X_train, X_test, y_train, y_test, "XGBoost")

    # LightGBM
    lgbm_model = LGBMClassifier(n_estimators=200, max_depth=5, random_state=42)
    train_and_evaluate_model(lgbm_model, X_train, X_test, y_train, y_test, "LightGBM")

