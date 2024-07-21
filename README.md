# F1-Milestone-Tharval-Amiraslani-Tharval-Korbin

## Introduction
Formula 1, often hailed as the pinnacle of motorsport, combines high-speed competition, cutting-edge technology, and a global fan base. This premier racing series showcases a unique synergy of engineering excellence, driver skill, and strategic depth, making it an intriguing subject for in-depth analysis. This project leverages machine learning to explore the complexities of Formula 1, with a primary focus on predicting top 10 finishes in races — a crucial outcome for securing championship points for drivers and teams. Additionally, we analyze race track characteristics through unsupervised learning, clustering tracks based on factors such as layout, surface, and length.

Historically, Formula 1 analytics has focused on predicting race winners using various statistical and machine learning techniques. These studies often reflect the dominance of certain teams during specific eras, such as Red Bull's supremacy from 2010 to 2013 and 2021 to the present, and Mercedes' stronghold from 2013 to 2020. While insightful, this focus on predicting race winners can overlook the nuanced dynamics and strategic elements that contribute to the broader competitive landscape of Formula 1 racing.

Our project aims to broaden the analytical scope by targeting the prediction of top 10 finishes. Securing a place in the top 10 is vital for accumulating championship points, crucial for both drivers and constructors over a season. By shifting our focus to these positions, we aim to capture the intricate competitive interactions and strategic nuances that influence race outcomes beyond the winner's podium. This approach provides a more comprehensive understanding of performance determinants in Formula 1, addressing both the predictability associated with team dominance and the variability arising from tactical decisions across the grid.

Additionally, our exploration into the unsupervised clustering of race tracks adds another dimension to this analysis. By uncovering similarities and distinctions among tracks, we offer deeper insights into how different track characteristics influence racing strategies and outcomes. This dual-faceted approach distinguishes our work from conventional race prediction analyses, offering a novel perspective on the dynamics of Formula 1 racing.

## Data

The Ergast Developer API serves as a pivotal data source for our Formula 1 analysis, offering an extensive repository of historical race data, including driver standings, race results, and qualifying times. Renowned for its comprehensive coverage of F1 statistics, the API has been instrumental in various analytical projects, ranging from predictive modeling to detailed statistical analyses of driver performances. In our project, the Ergast API provided a robust foundation for both the supervised and unsupervised learning components. We used it to extract datasets spanning from 1995 to the present, reflecting our focus on the modern era of Formula 1 racing. This period is characterized by significant technological advancements and regulatory changes, making the data particularly relevant for our analysis. Key features of the API that we leveraged include its ability to filter data by race season, event, and individual driver/team performance metrics. This flexibility allowed us to tailor our dataset precisely to the needs of our predictive models and clustering algorithms, ensuring a high degree of accuracy and relevance in our analysis.

> ##### Reproducibility Note
>
> The public Ergast API is set to sunset after the 2024 formula 1 season. To recreate You can find legacy data on [Kaggle](https://www.kaggle.com/datasets/rohanrao/formula-1-world-championship-1950-2020) or in this project's [GitHub repository](https://github.com/SarahAmiraslani/formula1-predictions-track-clustering/tree/main/data/raw).

## Methodology

Supervised learning is at the core of our project, focusing on predicting the likelihood of Formula 1 drivers finishing in the top 1 — an outcome critical for championship points. Our methodology involves training models on a dataset featuring variables such as track type, drivers' past performances, qualifying positions, and lap times. The goal is to classify each race outcome into a binary variable indicating whether a driver finishes in the top 10. This focus is driven by the significant impact these positions have on championship standings.

We employ various models, including Random Forest, Logistic Regression, and Neural Networks, each chosen for its ability to capture different aspects of the complex dynamics influencing Formula 1 race outcomes. These models are trained, validated, and tested on data from the Ergast Developer API, providing a robust foundation for our predictive analysis. Accuracy is our primary metric for evaluating model performance, as it offers clear interpretability for our binary classification task, simplifying the analysis by focusing on the overall goal of identifying drivers and conditions that frequently lead to top 10 finishes.

Our primary track clustering algorithm was DBSCAN (Density-Based Spatial Clustering of Applications with Noise). This unsupervised clustering algorithm is known for its efficacy in identifying non-normally distributed clusters of varying shapes and sizes without predefining the number of clusters. The optimal epsilon value, a critical hyperparameter for DBSCAN, was estimated using the Nearest Neighbors algorithm. We adhered to the heuristic of setting `n_neighbors` to `2*dim - 1`, where `dim` represents the dimensionality of our feature space, to balance local density estimation and computational feasibility. We determined the optimal epsilon value by evaluating where the distance of the 5th nearest neighbor stated to increase exponentially in the K-distance graph.

## Code

## Results

## Discussion

## References

## Contributors

- [Sarah Amiraslani](samirasl@umich.edu)
- [Akshay Tharval](tharval@umich.edu)
- [Sam Kobrin](kobrin@umich.edu)