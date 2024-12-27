import sqlite3
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.tree import DecisionTreeClassifier
from sklearn.ensemble import RandomForestClassifier
from sklearn import tree
import matplotlib.pyplot as plt

# Podłączenie do bazy danych i wczytanie danych
with sqlite3.connect('database.sqlite') as conn:
    query = """
    SELECT result,
           home_avg_overall, away_avg_overall,
           home_avg_defensive_work_rate, away_avg_defensive_work_rate,
           home_avg_attacking_work_rate, away_avg_attacking_work_rate
    FROM Match_with_overall
    """
    data = pd.read_sql_query(query, conn)

# Usunięcie brakujących danych
data = data.dropna()

# Tworzenie zestawów cech
data['overall_diff'] = data['home_avg_overall'] - data['away_avg_overall']
data['defensive_diff'] = data['home_avg_defensive_work_rate'] - data['away_avg_defensive_work_rate']
data['attacking_diff'] = data['home_avg_attacking_work_rate'] - data['away_avg_attacking_work_rate']

# Finalny zestaw cech (łączymy wszystkie różnice)
X = data[['overall_diff', 'defensive_diff', 'attacking_diff']]
y = data['result']

# Podział na zbiory treningowe i testowe
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

# Tworzenie modelu drzewa decyzyjnego
clf_tree = DecisionTreeClassifier(max_depth=4, random_state=42, class_weight='balanced')
clf_tree.fit(X_train, y_train)

# Wyświetlenie dokładności modelu drzewa decyzyjnego
accuracy_tree = clf_tree.score(X_test, y_test)
print(f"Dokładność drzewa decyzyjnego (class_weight='balanced'): {accuracy_tree:.2f}")

# Wizualizacja drzewa decyzyjnego
plt.figure(figsize=(60, 30))
tree.plot_tree(
    clf_tree,
    feature_names=X.columns,
    class_names=["Draw", "Home Win", "Away Win"],
    filled=True,
    rounded=True,
    fontsize=12
)
plt.title("Drzewo Decyzyjne - Wyniki Meczów", fontsize=16)
plt.show()

# Tworzenie modelu Random Forest
clf_rf = RandomForestClassifier(n_estimators=200, max_depth=8, random_state=42, class_weight='balanced')
clf_rf.fit(X_train, y_train)

# Wyświetlenie dokładności modelu Random Forest
accuracy_rf = clf_rf.score(X_test, y_test)
print(f"Dokładność Random Forest (połączenie cech): {accuracy_rf:.2f}")

# Feature importance dla Random Forest
importances = pd.Series(clf_rf.feature_importances_, index=X.columns)
sorted_importances = importances.sort_values(ascending=False)

# Rysowanie wykresu słupkowego z wartościami cech
plt.figure(figsize=(12, 6))
bars = sorted_importances.plot(kind='bar', title="Feature Importance - Random Forest", color='skyblue')

# Dodanie wartości nad słupkami
for bar in bars.patches:
    plt.text(
        bar.get_x() + bar.get_width() / 2,  # Pozycja X
        bar.get_height() + 0.001,  # Pozycja Y
        f'{bar.get_height():.2f}',  # Wartość zaokrąglona do dwóch miejsc po przecinku
        ha='center', va='bottom', fontsize=10
    )

plt.ylabel("Importance")
plt.xlabel("Features")
plt.xticks(rotation=45)  # Obrót etykiet osi X
plt.tight_layout()
plt.show()
