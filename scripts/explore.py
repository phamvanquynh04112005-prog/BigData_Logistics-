import pandas as pd

df = pd.read_csv("DataCoSupplyChainDataset.csv", encoding="latin-1")
print("Số dòng, số cột:", df.shape)
print(df.columns.tolist())
print(df.head())

desc = pd.read_csv("DescriptionDataCoSupplyChain.csv", encoding="latin-1")
print(desc.head(20))