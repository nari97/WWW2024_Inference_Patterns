A Method for Assessing Inference Patterns Captured by Embedding Models in Knowledge Graphs

This repository contains the code for the research paper to be published in THE WEB CONFERENCE 2024, SINGAPORE
The data can be found in link: https://drive.google.com/drive/folders/1k8SeEARwV-hJk9yIDu_2kyjNWO3RjXTY?usp=sharing

To run the code and recreate experiments:

1. Collect predictions - Run AugmentedKGE/AugmentedKGE/MaterializeRanks.py
2. Filter Top-k predictions - Run Python/FilterTopK.py
3. Run AMIE Inference pattern analysis - Run GradleProjects/Inference/EvaluateAMIEPatterns
4. Run Asymmetry Inference Pattern analysis - Run GradleProjects/Inference/EvaluateAsymmetryPatterns
5. Run Intersection Inference Pattern analysis - Run GradleProjects/Inference/EvaluateIntersectionPatterns
6. Compute results - Run GradleProjects/Inference/ComputeResults

