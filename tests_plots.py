import sqlite3
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score, classification_report, confusion_matrix
import matplotlib.pyplot as plt
import seaborn as sns

# Connect to the database and load data
with sqlite3.connect('database.sqlite') as conn:
    query = "SELECT * FROM Match_with_overall"
    data = pd.read_sql_query(query, conn)

# Remove rows with missing values
data = data.dropna()

# Remove duplicate columns
data = data.loc[:, ~data.columns.duplicated()]

# Remove draws
data = data[data['prediction'] != 0]

# Separate Home and Away features
home_features = [col for col in data.columns if col.startswith('home_avg_')]
away_features = [col for col in data.columns if col.startswith('away_avg_')]

# Calculate differences for all corresponding Home and Away features
for home_col, away_col in zip(home_features, away_features):
    feature_name = home_col.replace('home_avg_', '')
    diff_col_name = f'diff_{feature_name}'
    data[diff_col_name] = data[home_col] - data[away_col]

data['field_importance'] = abs(data['home_avg_overall_rating'] - data['away_avg_overall_rating'])

# Use only difference features and other relevant features
diff_features = [col for col in data.columns if col.startswith('diff_')]
feature_columns = diff_features + ['field_importance']

# Ensure equal representation of classes in the dataset
class_counts = data['prediction'].value_counts()
min_class_count = class_counts.min()

# Downsample classes to have equal representation
balanced_data = pd.concat([
    data[data['prediction'] == label].sample(n=min_class_count, random_state=42)
    for label in class_counts.index
])

# Shuffle the balanced data
balanced_data = balanced_data.sample(frac=1, random_state=42).reset_index(drop=True)

# Update X and y with balanced data
X = balanced_data[feature_columns]
y = balanced_data['prediction']

# Split the dataset into training and testing sets with stratification
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42, stratify=y)

# Define and train Random Forest classifier
rf_classifier = RandomForestClassifier(n_estimators=50, max_depth=6, random_state=42, class_weight='balanced')
rf_classifier.fit(X_train, y_train)

# Predict on the test set
y_pred = rf_classifier.predict(X_test)

# Calculate metrics
accuracy = accuracy_score(y_test, y_pred)
print(f"Random Forest Accuracy: {accuracy:.2f}")

print("\nClassification Report:")
print(classification_report(y_test, y_pred))

# Confusion Matrix
conf_matrix = confusion_matrix(y_test, y_pred)
plt.figure(figsize=(8, 6))
sns.heatmap(conf_matrix, annot=True, fmt='d', cmap='Blues',
            xticklabels=["Home Win", "Away Win"],
            yticklabels=["Home Win", "Away Win"])
plt.xlabel("Predicted")
plt.ylabel("Actual")
plt.title("Confusion Matrix - Random Forest")
plt.show()

# Feature Importances
feature_importances = rf_classifier.feature_importances_

# Create a DataFrame for better visualization
importances_df = pd.DataFrame({
    'Feature': feature_columns,
    'Importance': feature_importances
}).sort_values(by='Importance', ascending=False)

top_features = importances_df.head(10)
print("Most Important Features:")
for _, row in importances_df.iterrows():
    print(f"{row['Feature']}: {row['Importance']:.4f}")

# Visualize feature importances
plt.figure(figsize=(10, 6))
plt.barh(top_features['Feature'], top_features['Importance'], color='skyblue')
plt.xlabel('Feature Importance')
plt.ylabel('Feature')
plt.title('Top 10 Most Important Features')
plt.gca().invert_yaxis()  # Reverse the order for better readability
plt.show()
