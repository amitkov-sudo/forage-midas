package com.jpmc.midascore.entity;

import jakarta.persistence.*;

@Entity
@Table(name = "ledger_transaction")
public class TransactionRecord {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @ManyToOne(optional = false)
    @JoinColumn(name = "sender_id", nullable = false)
    private UserRecord sender;

    @ManyToOne(optional = false)
    @JoinColumn(name = "recipient_id", nullable = false)
    private UserRecord recipient;

    @Column(nullable = false)
    private float amount;

    protected TransactionRecord() {
    }

    public TransactionRecord(UserRecord sender, UserRecord recipient, float amount) {
        this.sender = sender;
        this.recipient = recipient;
        this.amount = amount;
    }

    public Long getId() {
        return id;
    }

    public UserRecord getSender() {
        return sender;
    }

    public UserRecord getRecipient() {
        return recipient;
    }

    public float getAmount() {
        return amount;
    }

    /** Sender display name (for logs / debugger). */
    public String getSenderName() {
        return sender != null ? sender.getName() : null;
    }

    /** Recipient display name (for logs / debugger). */
    public String getRecipientName() {
        return recipient != null ? recipient.getName() : null;
    }

    /**
     * Human-readable line: both account names and the transfer amount (the persisted transaction).
     */
    @Override
    public String toString() {
        return String.format(
                "TransactionRecord{id=%s, senderName='%s', recipientName='%s', transaction{amount=%f}}",
                id,
                getSenderName(),
                getRecipientName(),
                amount);
    }
}
