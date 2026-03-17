"""Initial schema - alerts, cases, case_events, transactions_summary, model_versions

Revision ID: 001
Revises: 
Create Date: 2026-03-17

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = '001'
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # ==========================================================================
    # Cases table (must be created first due to foreign key from alerts)
    # ==========================================================================
    op.create_table(
        'cases',
        sa.Column('case_id', sa.String(50), primary_key=True),
        sa.Column('title', sa.String(300), nullable=False),
        sa.Column('description', sa.Text(), default=''),
        sa.Column('status', sa.String(50), default='new', index=True),
        sa.Column('priority', sa.String(10), default='P3', index=True),
        
        # Fraud classification
        sa.Column('primary_fraud_type', sa.String(50), nullable=True),
        sa.Column('fraud_types', postgresql.ARRAY(sa.String()), default=[]),
        
        # Aggregated metrics
        sa.Column('alert_count', sa.Integer(), default=0),
        sa.Column('total_amount', sa.Float(), default=0.0),
        sa.Column('max_severity', sa.String(20), default='medium'),
        sa.Column('avg_confidence', sa.Float(), default=0.0),
        
        # Related entities
        sa.Column('involved_accounts', postgresql.ARRAY(sa.String()), default=[]),
        sa.Column('involved_transactions', postgresql.ARRAY(sa.String()), default=[]),
        
        # Assignment
        sa.Column('assigned_to', sa.String(100), nullable=True, index=True),
        sa.Column('assigned_team', sa.String(100), nullable=True),
        
        # Tags
        sa.Column('tags', postgresql.ARRAY(sa.String()), default=[]),
        
        # Timestamps
        sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.func.now(), index=True),
        sa.Column('updated_at', sa.DateTime(timezone=True), server_default=sa.func.now(), onupdate=sa.func.now()),
        sa.Column('first_alert_at', sa.DateTime(timezone=True), nullable=True),
        sa.Column('last_alert_at', sa.DateTime(timezone=True), nullable=True),
        
        # SLA
        sa.Column('sla_due_at', sa.DateTime(timezone=True), nullable=True),
        sa.Column('sla_breached', sa.Boolean(), default=False),
        
        # Resolution
        sa.Column('resolution', sa.Text(), nullable=True),
        sa.Column('resolved_at', sa.DateTime(timezone=True), nullable=True),
        sa.Column('resolved_by', sa.String(100), nullable=True),
        
        # Compliance
        sa.Column('str_required', sa.Boolean(), default=False),
        sa.Column('str_filed', sa.Boolean(), default=False),
        sa.Column('str_filed_at', sa.DateTime(timezone=True), nullable=True),
        sa.Column('str_reference', sa.String(100), nullable=True),
        
        # Notes
        sa.Column('notes_count', sa.Integer(), default=0),
        sa.Column('last_note', sa.Text(), nullable=True),
    )
    
    op.create_index('ix_cases_status_priority', 'cases', ['status', 'priority'])
    op.create_index('ix_cases_created_at_desc', 'cases', [sa.text('created_at DESC')])
    
    # ==========================================================================
    # Alerts table
    # ==========================================================================
    op.create_table(
        'alerts',
        sa.Column('alert_id', sa.String(50), primary_key=True),
        
        # Fraud classification
        sa.Column('fraud_type', sa.String(50), nullable=False, index=True),
        sa.Column('severity', sa.String(20), nullable=False, index=True),
        sa.Column('confidence', sa.Float(), nullable=False),
        
        # Transaction context
        sa.Column('transaction_id', sa.String(50), nullable=False, index=True),
        sa.Column('sender_iban', sa.String(34), nullable=False, index=True),
        sa.Column('sender_name', sa.String(200), nullable=False),
        sa.Column('sender_city', sa.String(100), default=''),
        sa.Column('receiver_iban', sa.String(34), nullable=False, index=True),
        sa.Column('receiver_name', sa.String(200), nullable=False),
        sa.Column('receiver_city', sa.String(100), default=''),
        sa.Column('amount', sa.Float(), nullable=False),
        sa.Column('currency', sa.String(3), default='TRY'),
        
        # Description
        sa.Column('title', sa.String(200), default=''),
        sa.Column('description', sa.Text(), default=''),
        
        # Evidence (JSONB)
        sa.Column('evidence', postgresql.JSONB(), default={}),
        
        # Relations
        sa.Column('related_transactions', postgresql.ARRAY(sa.String()), default=[]),
        sa.Column('related_accounts', postgresql.ARRAY(sa.String()), default=[]),
        sa.Column('case_id', sa.String(50), sa.ForeignKey('cases.case_id', ondelete='SET NULL'), nullable=True, index=True),
        
        # Timestamps
        sa.Column('detected_at', sa.DateTime(timezone=True), server_default=sa.func.now(), index=True),
        sa.Column('updated_at', sa.DateTime(timezone=True), server_default=sa.func.now(), onupdate=sa.func.now()),
        
        # Status
        sa.Column('is_dismissed', sa.Boolean(), default=False),
        sa.Column('dismissed_by', sa.String(100), nullable=True),
        sa.Column('dismissed_at', sa.DateTime(timezone=True), nullable=True),
        sa.Column('dismissed_reason', sa.Text(), nullable=True),
        
        # Metadata
        sa.Column('detector_versions', postgresql.JSONB(), default={}),
        sa.Column('processing_time_ms', sa.Float(), default=0.0),
    )
    
    op.create_index('ix_alerts_detected_at_desc', 'alerts', [sa.text('detected_at DESC')])
    op.create_index('ix_alerts_fraud_severity', 'alerts', ['fraud_type', 'severity'])
    op.create_index('ix_alerts_sender_receiver', 'alerts', ['sender_iban', 'receiver_iban'])
    
    # ==========================================================================
    # Case Events table (audit log)
    # ==========================================================================
    op.create_table(
        'case_events',
        sa.Column('event_id', sa.String(50), primary_key=True),
        sa.Column('case_id', sa.String(50), sa.ForeignKey('cases.case_id', ondelete='CASCADE'), nullable=False, index=True),
        
        # Event type
        sa.Column('event_type', sa.String(50), nullable=False, index=True),
        
        # Actor
        sa.Column('actor', sa.String(100), default='system'),
        sa.Column('actor_type', sa.String(20), default='system'),
        
        # Change details
        sa.Column('description', sa.Text(), default=''),
        sa.Column('previous_value', sa.Text(), nullable=True),
        sa.Column('new_value', sa.Text(), nullable=True),
        
        # Extra data
        sa.Column('extra_data', postgresql.JSONB(), default={}),
        
        # Related entities
        sa.Column('alert_id', sa.String(50), nullable=True),
        sa.Column('transaction_id', sa.String(50), nullable=True),
        
        # Timestamp
        sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.func.now(), index=True),
        
        # Audit fields
        sa.Column('ip_address', sa.String(45), nullable=True),
        sa.Column('user_agent', sa.Text(), nullable=True),
    )
    
    op.create_index('ix_case_events_case_created', 'case_events', ['case_id', sa.text('created_at DESC')])
    
    # ==========================================================================
    # Transactions Summary table
    # ==========================================================================
    op.create_table(
        'transactions_summary',
        sa.Column('transaction_id', sa.String(50), primary_key=True),
        
        # Core fields
        sa.Column('sender_iban', sa.String(34), nullable=False, index=True),
        sa.Column('sender_name', sa.String(200), nullable=False),
        sa.Column('sender_city', sa.String(100), default=''),
        sa.Column('receiver_iban', sa.String(34), nullable=False, index=True),
        sa.Column('receiver_name', sa.String(200), nullable=False),
        sa.Column('receiver_city', sa.String(100), default=''),
        sa.Column('amount', sa.Float(), nullable=False),
        sa.Column('currency', sa.String(3), default='TRY'),
        sa.Column('description', sa.Text(), default=''),
        sa.Column('timestamp', sa.DateTime(timezone=True), nullable=False, index=True),
        sa.Column('channel', sa.String(20), default='web'),
        
        # Processing results
        sa.Column('is_fraud', sa.Boolean(), default=False, index=True),
        sa.Column('fraud_score', sa.Float(), default=0.0),
        sa.Column('alert_ids', postgresql.ARRAY(sa.String()), default=[]),
        sa.Column('case_id', sa.String(50), nullable=True),
        
        # Metadata
        sa.Column('processed_at', sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.Column('processing_time_ms', sa.Float(), default=0.0),
        sa.Column('detector_versions', postgresql.JSONB(), default={}),
    )
    
    op.create_index('ix_txn_timestamp_desc', 'transactions_summary', [sa.text('timestamp DESC')])
    op.create_index('ix_txn_fraud', 'transactions_summary', ['is_fraud', sa.text('timestamp DESC')])
    
    # ==========================================================================
    # Model Versions table (ML tracking)
    # ==========================================================================
    op.create_table(
        'model_versions',
        sa.Column('version_id', sa.String(50), primary_key=True),
        
        # Model info
        sa.Column('model_name', sa.String(100), nullable=False, index=True),
        sa.Column('version', sa.String(20), nullable=False),
        
        # Status
        sa.Column('stage', sa.String(20), default='development', index=True),
        sa.Column('is_active', sa.Boolean(), default=False, index=True),
        
        # Metrics
        sa.Column('metrics', postgresql.JSONB(), default={}),
        
        # Training info
        sa.Column('training_dataset', sa.String(200), nullable=True),
        sa.Column('training_samples', sa.Integer(), default=0),
        sa.Column('training_time_seconds', sa.Float(), default=0.0),
        sa.Column('hyperparameters', postgresql.JSONB(), default={}),
        
        # Artifact
        sa.Column('artifact_path', sa.String(500), nullable=True),
        sa.Column('artifact_hash', sa.String(64), nullable=True),
        
        # Timestamps
        sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.Column('deployed_at', sa.DateTime(timezone=True), nullable=True),
        
        # Metadata
        sa.Column('description', sa.Text(), default=''),
        sa.Column('created_by', sa.String(100), default='system'),
    )


def downgrade() -> None:
    op.drop_table('model_versions')
    op.drop_table('transactions_summary')
    op.drop_table('case_events')
    op.drop_table('alerts')
    op.drop_table('cases')
