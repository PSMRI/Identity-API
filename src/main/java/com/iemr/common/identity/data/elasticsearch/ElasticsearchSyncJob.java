package com.iemr.common.identity.data.elasticsearch;

import java.sql.Timestamp;
import java.math.BigInteger;
import jakarta.persistence.*;
import lombok.Data;

/**
 * Entity to track Elasticsearch sync job status
 */
@Entity
@Table(name = "t_elasticsearch_sync_job")
@Data
public class ElasticsearchSyncJob {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "job_id")
    private Long jobId;

    @Column(name = "job_type", length = 50, nullable = false)
    private String jobType; // FULL_SYNC, INCREMENTAL_SYNC, SINGLE_BENEFICIARY

    @Column(name = "status", length = 50, nullable = false)
    private String status; // PENDING, RUNNING, COMPLETED, FAILED, CANCELLED

    @Column(name = "total_records")
    private Long totalRecords;

    @Column(name = "processed_records")
    private Long processedRecords;

    @Column(name = "success_count")
    private Long successCount;

    @Column(name = "failure_count")
    private Long failureCount;

    @Column(name = "current_offset")
    private Integer currentOffset;

    @Column(name = "started_at")
    private Timestamp startedAt;

    @Column(name = "completed_at")
    private Timestamp completedAt;

    @Column(name = "error_message", columnDefinition = "TEXT")
    private String errorMessage;

    @Column(name = "triggered_by", length = 100)
    private String triggeredBy; // User who triggered the job

    @Column(name = "created_date", nullable = false, updatable = false)
    private Timestamp createdDate;

    @Column(name = "last_updated")
    private Timestamp lastUpdated;

    @Column(name = "estimated_time_remaining")
    private Long estimatedTimeRemaining; // in seconds

    @Column(name = "processing_speed")
    private Double processingSpeed; // records per second

    @PrePersist
    protected void onCreate() {
        createdDate = new Timestamp(System.currentTimeMillis());
        lastUpdated = createdDate;
    }

    @PreUpdate
    protected void onUpdate() {
        lastUpdated = new Timestamp(System.currentTimeMillis());
    }

    /**
     * Calculate progress percentage
     */
    @Transient
    public double getProgressPercentage() {
        if (totalRecords == null || totalRecords == 0) {
            return 0.0;
        }
        return (processedRecords * 100.0) / totalRecords;
    }

    /**
     * Check if job is active (running)
     */
    @Transient
    public boolean isActive() {
        return "RUNNING".equals(status) || "PENDING".equals(status);
    }
}