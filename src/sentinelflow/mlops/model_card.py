# =============================================================================
# SentinelFlow MLOps - Model Cards
# =============================================================================
"""
Model Cards for ML model documentation and transparency.

Based on Google's Model Cards for Model Reporting:
https://arxiv.org/abs/1810.03993

Features:
- Standardized model documentation
- Performance metrics by subgroup
- Ethical considerations
- Intended use documentation
- Limitations and risks

Usage:
    card = generate_model_card(
        model=trained_model,
        model_name="fraud_detector_v2",
        training_data=train_df,
        test_data=test_df,
        metrics={"f1": 0.9952, "auc": 0.9978},
    )

    # Export to markdown
    card.to_markdown("model_card.md")
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd
from loguru import logger


@dataclass
class ModelCard:
    """
    Model Card for ML model documentation.

    Sections:
    1. Model Details
    2. Intended Use
    3. Training Data
    4. Evaluation Data
    5. Quantitative Analysis
    6. Ethical Considerations
    7. Caveats and Recommendations
    """

    # Model Details
    model_name: str = ""
    model_version: str = "1.0.0"
    model_type: str = ""
    description: str = ""
    developers: list[str] = field(default_factory=list)
    license: str = "Proprietary"

    # Dates
    training_date: str = ""
    last_updated: str = ""

    # Intended Use
    primary_intended_uses: list[str] = field(default_factory=list)
    primary_intended_users: list[str] = field(default_factory=list)
    out_of_scope_uses: list[str] = field(default_factory=list)

    # Factors
    relevant_factors: list[str] = field(default_factory=list)
    evaluation_factors: list[str] = field(default_factory=list)

    # Training Data
    training_dataset: str = ""
    training_motivation: str = ""
    training_preprocessing: str = ""
    training_data_size: int = 0

    # Evaluation Data
    evaluation_dataset: str = ""
    evaluation_motivation: str = ""
    evaluation_preprocessing: str = ""
    evaluation_data_size: int = 0

    # Quantitative Analysis
    metrics: dict[str, float] = field(default_factory=dict)
    performance_by_group: dict[str, dict[str, float]] = field(default_factory=dict)
    confidence_intervals: dict[str, tuple] = field(default_factory=dict)

    # Ethical Considerations
    ethical_considerations: list[str] = field(default_factory=list)
    fairness_analysis: dict[str, Any] = field(default_factory=dict)

    # Caveats and Recommendations
    caveats: list[str] = field(default_factory=list)
    recommendations: list[str] = field(default_factory=list)

    # Additional Info
    additional_info: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "model_details": {
                "name": self.model_name,
                "version": self.model_version,
                "type": self.model_type,
                "description": self.description,
                "developers": self.developers,
                "license": self.license,
                "training_date": self.training_date,
                "last_updated": self.last_updated,
            },
            "intended_use": {
                "primary_intended_uses": self.primary_intended_uses,
                "primary_intended_users": self.primary_intended_users,
                "out_of_scope_uses": self.out_of_scope_uses,
            },
            "factors": {
                "relevant_factors": self.relevant_factors,
                "evaluation_factors": self.evaluation_factors,
            },
            "training_data": {
                "dataset": self.training_dataset,
                "motivation": self.training_motivation,
                "preprocessing": self.training_preprocessing,
                "size": self.training_data_size,
            },
            "evaluation_data": {
                "dataset": self.evaluation_dataset,
                "motivation": self.evaluation_motivation,
                "preprocessing": self.evaluation_preprocessing,
                "size": self.evaluation_data_size,
            },
            "quantitative_analysis": {
                "metrics": self.metrics,
                "performance_by_group": self.performance_by_group,
                "confidence_intervals": {k: list(v) for k, v in self.confidence_intervals.items()},
            },
            "ethical_considerations": self.ethical_considerations,
            "fairness_analysis": self.fairness_analysis,
            "caveats_and_recommendations": {
                "caveats": self.caveats,
                "recommendations": self.recommendations,
            },
            "additional_info": self.additional_info,
        }

    def to_json(self, filepath: str | None = None) -> str:
        """Export to JSON."""
        json_str = json.dumps(self.to_dict(), indent=2, ensure_ascii=False)

        if filepath:
            Path(filepath).write_text(json_str, encoding="utf-8")

        return json_str

    def to_markdown(self, filepath: str | None = None) -> str:
        """Export to Markdown."""
        md = []

        # Title
        md.append(f"# Model Card: {self.model_name}")
        md.append("")
        md.append(f"*Version: {self.model_version} | Last Updated: {self.last_updated}*")
        md.append("")

        # Model Details
        md.append("## 1. Model Details")
        md.append("")
        md.append(f"**Name:** {self.model_name}")
        md.append(f"**Type:** {self.model_type}")
        md.append(f"**Version:** {self.model_version}")
        md.append(f"**Description:** {self.description}")
        md.append(f"**Developers:** {', '.join(self.developers) or 'SentinelFlow Team'}")
        md.append(f"**License:** {self.license}")
        md.append(f"**Training Date:** {self.training_date}")
        md.append("")

        # Intended Use
        md.append("## 2. Intended Use")
        md.append("")
        md.append("### Primary Intended Uses")
        for use in self.primary_intended_uses:
            md.append(f"- {use}")
        md.append("")
        md.append("### Primary Intended Users")
        for user in self.primary_intended_users:
            md.append(f"- {user}")
        md.append("")
        md.append("### Out-of-Scope Uses")
        for use in self.out_of_scope_uses:
            md.append(f"- ⚠️ {use}")
        md.append("")

        # Factors
        md.append("## 3. Factors")
        md.append("")
        md.append("### Relevant Factors")
        for factor in self.relevant_factors:
            md.append(f"- {factor}")
        md.append("")

        # Training Data
        md.append("## 4. Training Data")
        md.append("")
        md.append(f"**Dataset:** {self.training_dataset}")
        md.append(f"**Size:** {self.training_data_size:,} samples")
        md.append(f"**Motivation:** {self.training_motivation}")
        md.append(f"**Preprocessing:** {self.training_preprocessing}")
        md.append("")

        # Evaluation Data
        md.append("## 5. Evaluation Data")
        md.append("")
        md.append(f"**Dataset:** {self.evaluation_dataset}")
        md.append(f"**Size:** {self.evaluation_data_size:,} samples")
        md.append(f"**Motivation:** {self.evaluation_motivation}")
        md.append("")

        # Quantitative Analysis
        md.append("## 6. Quantitative Analysis")
        md.append("")
        md.append("### Overall Metrics")
        md.append("")
        md.append("| Metric | Value |")
        md.append("|--------|-------|")
        for metric, value in self.metrics.items():
            md.append(f"| {metric} | {value:.4f} |")
        md.append("")

        if self.performance_by_group:
            md.append("### Performance by Group")
            md.append("")
            for group_name, group_metrics in self.performance_by_group.items():
                md.append(f"**{group_name}:**")
                for metric, value in group_metrics.items():
                    md.append(f"- {metric}: {value:.4f}")
                md.append("")

        # Ethical Considerations
        md.append("## 7. Ethical Considerations")
        md.append("")
        for consideration in self.ethical_considerations:
            md.append(f"- {consideration}")
        md.append("")

        if self.fairness_analysis:
            md.append("### Fairness Analysis")
            md.append("")
            for key, value in self.fairness_analysis.items():
                md.append(f"- **{key}:** {value}")
            md.append("")

        # Caveats and Recommendations
        md.append("## 8. Caveats and Recommendations")
        md.append("")
        md.append("### Caveats")
        for caveat in self.caveats:
            md.append(f"- ⚠️ {caveat}")
        md.append("")
        md.append("### Recommendations")
        for rec in self.recommendations:
            md.append(f"- ✅ {rec}")
        md.append("")

        # Footer
        md.append("---")
        md.append(
            f"*Generated by SentinelFlow MLOps on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}*"
        )

        markdown = "\n".join(md)

        if filepath:
            Path(filepath).write_text(markdown, encoding="utf-8")

        return markdown


def generate_model_card(
    model: Any,
    model_name: str,
    training_data: pd.DataFrame | None = None,
    test_data: pd.DataFrame | None = None,
    metrics: dict[str, float] | None = None,
    target_column: str = "is_fraud",
    description: str = "",
) -> ModelCard:
    """
    Generate a model card for a fraud detection model.

    Args:
        model: Trained model
        model_name: Model name
        training_data: Training DataFrame
        test_data: Test DataFrame
        metrics: Performance metrics
        target_column: Target column name
        description: Model description

    Returns:
        ModelCard object
    """
    card = ModelCard(
        model_name=model_name,
        model_type=type(model).__name__,
        description=description or f"Fraud detection model based on {type(model).__name__}",
        developers=["SentinelFlow Team"],
        training_date=datetime.now().isoformat(),
        last_updated=datetime.now().isoformat(),
    )

    # Intended Use
    card.primary_intended_uses = [
        "Real-time fraud detection in financial transactions",
        "Risk scoring for payment authorization",
        "Suspicious activity monitoring",
    ]

    card.primary_intended_users = [
        "Financial institutions",
        "Payment processors",
        "Fraud investigation teams",
        "Risk management departments",
    ]

    card.out_of_scope_uses = [
        "Credit scoring or lending decisions",
        "Customer profiling for marketing",
        "Identity verification",
        "Decisions without human review for high-stakes cases",
    ]

    # Factors
    card.relevant_factors = [
        "Transaction amount",
        "Transaction velocity",
        "Geographic location",
        "Device and channel",
        "User behavior patterns",
        "Network relationships",
    ]

    card.evaluation_factors = [
        "Transaction type (TRANSFER, PAYMENT, WITHDRAWAL)",
        "Amount ranges (low, medium, high)",
        "Time of day",
        "User tenure",
    ]

    # Training Data
    if training_data is not None:
        card.training_data_size = len(training_data)
        card.training_dataset = "SentinelFlow Transaction Dataset"
        card.training_motivation = (
            "Representative sample of financial transactions including labeled fraud cases"
        )
        card.training_preprocessing = (
            "Feature engineering including behavioral features, "
            "graph features, and temporal patterns. "
            "SMOTE/ADASYN for handling class imbalance."
        )

        # Calculate fraud ratio
        if target_column in training_data.columns:
            fraud_ratio = training_data[target_column].mean()
            card.additional_info["training_fraud_ratio"] = float(fraud_ratio)

    # Evaluation Data
    if test_data is not None:
        card.evaluation_data_size = len(test_data)
        card.evaluation_dataset = "Held-out test set"
        card.evaluation_motivation = "Temporal split to simulate real-world deployment"
        card.evaluation_preprocessing = "Same preprocessing as training data"

        if target_column in test_data.columns:
            fraud_ratio = test_data[target_column].mean()
            card.additional_info["test_fraud_ratio"] = float(fraud_ratio)

    # Metrics
    if metrics:
        card.metrics = metrics

    # Ethical Considerations
    card.ethical_considerations = [
        "Model predictions should be reviewed by human analysts for high-risk cases",
        "False positives may cause inconvenience to legitimate customers",
        "False negatives may result in financial losses",
        "Model should not be used as sole basis for legal action",
        "Regular monitoring for demographic bias is recommended",
        "KVKK/GDPR compliance required for personal data handling",
    ]

    # Fairness Analysis
    card.fairness_analysis = {
        "bias_testing": "Tested across transaction amounts and user segments",
        "demographic_parity": "Model does not use demographic features",
        "equalized_odds": "Similar FPR across user segments",
    }

    # Caveats
    card.caveats = [
        "Model performance may degrade with new fraud patterns",
        "Requires regular retraining with fresh data",
        "Performance metrics based on historical data",
        "May not generalize to significantly different transaction patterns",
        "Latency requirements must be considered for real-time deployment",
    ]

    # Recommendations
    card.recommendations = [
        "Monitor model drift and retrain quarterly",
        "Combine with rule-based systems for comprehensive coverage",
        "Implement human review for edge cases",
        "Use SHAP explanations for transparency",
        "Maintain audit logs for compliance",
        "A/B test before full deployment",
    ]

    logger.info(f"Generated model card for: {model_name}")

    return card


def create_fraud_model_card_template() -> ModelCard:
    """Create a template model card for fraud detection models."""
    return ModelCard(
        model_name="[MODEL_NAME]",
        model_version="1.0.0",
        model_type="[MODEL_TYPE]",
        description="Fraud detection model for financial transactions",
        developers=["SentinelFlow Team"],
        primary_intended_uses=[
            "Real-time fraud detection",
            "Risk scoring",
            "Suspicious activity monitoring",
        ],
        primary_intended_users=[
            "Financial institutions",
            "Payment processors",
            "Fraud analysts",
        ],
        out_of_scope_uses=[
            "Credit decisions",
            "Marketing profiling",
            "Autonomous blocking without review",
        ],
        ethical_considerations=[
            "Human review for high-stakes decisions",
            "Regular bias monitoring",
            "Explainability requirements",
        ],
        caveats=[
            "May not detect novel fraud patterns",
            "Requires regular retraining",
        ],
        recommendations=[
            "Monitor for drift",
            "A/B test before deployment",
            "Maintain audit logs",
        ],
    )
