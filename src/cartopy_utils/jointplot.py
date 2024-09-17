
import seaborn as sns
import matplotlib.pyplot as plt

penguins = sns.load_dataset("penguins")
print(penguins)
sns.jointplot(data=penguins, x="bill_length_mm", y="bill_depth_mm", kind="hex")
plt.show()