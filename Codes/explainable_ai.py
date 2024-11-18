import shap

explainer = shap.LinearExplainer([1, 0.5, -1], [[1.0, 0.5, -1.0]])
shap_values = explainer.shap_values([2.0, 1.0, 1.0])
print(shap_values)
