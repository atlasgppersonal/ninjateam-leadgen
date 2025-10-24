# Server-Side Migration Plan: Lead Generation System

## 1. Introduction

This document outlines the current architecture of the lead generation system, detailing the interaction between the main application (`lead_gen_main.py`), the background processor (`contact-extractor.py`), and the email sender (`email-sender.py`). It also details the database schemas involved. The primary goal of this document is to propose a server-side migration strategy using Firebase Functions, addressing scalability, reliability, and separation of concerns, while incorporating the existing LLM Governor and planning for data migration.

## 2. Current Architecture Overview

The current system operates with Python scripts managing the lead generation workflow.

### 2.1. `lead_gen_main.py` (Main Application)

*   **Role**: Acts as the primary user interface and orchestrator for initiating lead generation tasks. This will remain the **sole client-side component**.
*   **Key Functions**:
    *   Provides a command-line interface for user interaction (campaign control, manual CAPTCHA processing).
    *   Loads configuration from `config.json` (which will be managed server-side via dashboard).
    *   Manages campaign parameters (city selection, email template selection, number of leads to harvest).
    *   Initiates lead harvesting and processing by calling server-side Firebase Functions.
    *   Facilitates manual CAPTCHA resolution by interacting with a server-side CAPTCHA Orchestrator Function.
    *   Manages Microsoft Outlook OAuth authentication flow, passing tokens securely to server-side functions.
    *   Interacts with the client-side dashboard's admin section to configure server-side settings, including LLM transaction limits.

### 2.2. `contact-extractor.py` (Background Processor) - **Migrated to Server-Side Functions**

*   **Role**: Handles the core data harvesting, scraping, enrichment, and initial processing of leads. This functionality will be implemented as server-side Firebase Functions.
*   **Key Functions (as Server-Side Functions)**:
    *   **Harvester Function**:
        *   **Trigger**: Cloud Scheduler or a write to Firestore/Realtime Database from `lead_gen_main.py`.
        *   **Logic**: Performs web scraping. Manages state (harvested URLs, duplicate checks) using Firestore. Processes leads in batches and triggers subsequent functions via Pub/Sub or Firestore writes.
    *   **Processor Function**:
        *   **Trigger**: Pub/Sub topic or Firestore/Realtime Database writes (new harvested URLs).
        *   **Logic**: Scrapes post bodies, performs initial phone number extraction and duplicate checks against a cloud-based contacts store. Uses LLM for enrichment and website scoring, routing requests through the LLM Governor. Triages leads into different queues/collections (e.g., for CAPTCHA, direct email processing).
    *   **LLM Integration**: All LLM calls will be routed through the server-side LLM Governor.
    *   **LLM Governor Interaction**: The LLM Governor will enforce configurable transaction limits (e.g., 20 transactions per minute), with the limit configurable via the client-side dashboard's admin section and stored in a cloud database (e.g., Firestore).

### 2.3. `email-sender.py` (Email Generator/Sender) - **Migrated to Server-Side Functions**

*   **Role**: Manages email templates, generates personalized email content, and sends emails to leads. This functionality will be implemented as a server-side "Email Agent" Firebase Function.
*   **Key Functions (as Server-Side Email Agent Function)**:
    *   **Email Queue Management**: Listens for new email queue entries (e.g., via Pub/Sub or Firestore triggers).
    *   **Template Management**: Fetches email templates and variations from a cloud database (e.g., Firestore).
    *   **Personalization**: Assembles personalized emails using templates, lead data, and signature blocks.
    *   **Email Sending**:
        *   Authenticates with Microsoft Graph API using securely stored tokens (managed via the server-side authentication flow).
        *   Sends emails with intervals and pause mechanisms to simulate human sending and avoid lockout.
        *   Supports future integration with other mass email systems and Google Business Accounts.
    *   **Authentication Handling**: Manages OAuth tokens for sending emails, including refreshing them as needed.

## 3. Database Schemas (Migrated to Cloud Database, e.g., Firestore)

The existing SQLite schemas will be adapted for a cloud-native database like Firestore.

### 3.1. `master_contacts.db` -> Firestore Collection: `contacts`

*   **`contacts` Document Structure**:
    *   `phone` (String): Normalized phone number.
    *   `name` (String): Contact's name.
    *   `email` (String): Contact's email address.
    *   `lastSent` (Timestamp): Timestamp of last communication.
    *   `sourceUrl` (String): URL where the lead was found.
    *   `imageHash` (String): Hash of an associated image for duplicate detection.
    *   `businessName` (String): Name of the business.
    *   `category` (String): Category of the business.
    *   `servicesRendered` (Array of Strings): Services offered.
    *   `status` (String): Current status of the lead (e.g., 'PROCESSED', 'PENDING_CAPTCHA').
    *   `city` (String): City associated with the lead.
    *   `leadDataJson` (Object): Original lead data as a JSON object.

### 3.2. `email_engine.db` -> Firestore Collections

*   **`processorQueue` Collection**: Manages requests for lead processing.
    *   **Document Structure**:
        *   `templateId` (Number): ID of the email template to use.
        *   `city` (String): The city for which leads are to be processed.
        *   `numberOfLeadsToProcess` (Number): The target number of leads for this batch.
        *   `requestTimestamp` (Timestamp): When the request was made.
        *   `status` (String): Status of the request (e.g., 'PENDING', 'PROCESSING', 'COMPLETED').

*   **`templates` Collection**: Stores base email templates.
    *   **Document Structure**:
        *   `templateId` (Number): Unique identifier for the template.
        *   `templateName` (String): A descriptive name for the template.
        *   `description` (String): A brief description of the template's purpose.
        *   `baseSubject` (String): The base subject line for emails.
        *   `baseBodyHtml` (String): The base HTML content for the email body.
        *   `isArchived` (Boolean): Flag indicating if the template is archived.
        *   `createdAt` (Timestamp): When the template was created.

*   **`emailQueue` Collection**: Holds leads ready to be emailed.
    *   **Document Structure**:
        *   `templateId` (Number): The ID of the template to use for this lead.
        *   `leadData` (Object): Lead data, including personalized fields.
        *   `city` (String): The city associated with the lead.
        *   `status` (String): Current status (e.g., 'QUEUED', 'SENT', 'ERROR_NO_EMAIL', 'ERROR_API_FAILURE').
        *   `createdAt` (Timestamp): When the lead was added to the queue.
        *   `sentAt` (Timestamp): When the email was successfully sent.
        *   `timezone` (String): The timezone of the lead's city, used for scheduling.

*   **`variationStorage` Collection**: Stores generated variations of email templates. (Note: Migration of this table is excluded from the initial phase as per instructions).

## 4. Current Interaction Flow (Client-Side `lead_gen_main.py`)

1.  **Initiation**: User interacts with `lead_gen_main.py` to start a campaign (select city, template, number of leads).
2.  **Server Request**: `lead_gen_main.py` makes an API call to a server-side Harvester Function (e.g., `POST /triggerHarvest`) with campaign parameters.
3.  **Authentication Flow**: If required, `lead_gen_main.py` initiates the OAuth flow for Microsoft Outlook, receives a callback, and securely passes the token/credentials to a server-side Authentication Function.
4.  **Monitoring & CAPTCHA Intervention**: `lead_gen_main.py` may poll server-side status endpoints or receive notifications (e.g., via WebSockets or Firestore listeners) about leads requiring CAPTCHA. It then calls a CAPTCHA Orchestrator Function to facilitate manual resolution.
5.  **Configuration Management**: `lead_gen_main.py` interacts with a server-side configuration service (accessed via dashboard) to set LLM transaction limits and other parameters.

## 5. Proposed Server-Side Migration (Firebase Functions)

The goal is to migrate the background processing and email sending logic to serverless Firebase Functions, making the system more scalable and manageable.

### 5.1. Rationale

*   **Scalability**: Firebase Functions scale automatically.
*   **Reliability**: Serverless architecture reduces single points of failure.
*   **Separation of Concerns**: Clear separation between client (`lead_gen_main.py`) and backend (Firebase Functions).
*   **Managed Infrastructure**: Firebase handles server management.
*   **Cost-Effectiveness**: Pay-as-you-go, managed by LLM Governor for API costs.

### 5.2. Component Mapping to Functions

*   **Harvester Function**:
    *   **Trigger**: Cloud Scheduler or Firestore write from `lead_gen_main.py`.
    *   **Logic**: Web scraping, state management (Firestore), batch processing, triggers Processor Function via Pub/Sub or Firestore.
    *   **Daemon-like Handling**: Achieved through batch processing and self-triggering/chaining of functions.

*   **Processor Function**:
    *   **Trigger**: Pub/Sub or Firestore writes (from Harvester).
    *   **Logic**: Lead scraping, LLM enrichment (via LLM Governor), triage. Manages long-running tasks via background execution or task chaining.
    *   **LLM Integration**: Routes all LLM calls through the LLM Governor.
    *   **LLM Governor Interaction**: Enforces configurable transaction limits (e.g., 20/min), configurable via client dashboard/admin section, stored in Firestore.

*   **Email Sender Function (Email Agent)**:
    *   **Trigger**: Pub/Sub or Firestore writes (new email queue entries).
    *   **Logic**: Manages email templates/variations (from Firestore), personalizes emails, sends via Microsoft Graph API using securely managed tokens. Supports future provider integrations.
    *   **Authentication**: Manages OAuth tokens, including refreshing them.

*   **CAPTCHA Resolution Orchestrator Function**:
    *   **Trigger**: API endpoint called by `lead_gen_main.py`.
    *   **Logic**: Manages manual CAPTCHA workflow, provides interface/URL for user, receives results, updates lead status, triggers further processing.

*   **Authentication Management Function**:
    *   **Trigger**: API endpoint for OAuth callback from client.
    *   **Logic**: Securely handles OAuth tokens, stores refresh tokens (e.g., in Firestore), provides token refresh mechanism for Email Sender Function.

*   **Configuration Management Function**:
    *   **Trigger**: API endpoint called by `lead_gen_main.py` (via dashboard).
    *   **Logic**: Manages and serves configuration settings (e.g., LLM transaction limits, active timezones, email provider settings) stored in Firestore.

### 5.3. New Interaction Model

`lead_gen_main.py` interacts with Firebase Functions via API calls and potentially Firestore listeners/triggers for status updates.

*   **Initiating Lead Generation**: `lead_gen_main.py` calls `POST /triggerHarvest` on the Harvester Function.
*   **Data Storage**: Uses Firestore for all data (contacts, queues, templates, configurations).
*   **Queueing**: Replaces SQLite queues with Pub/Sub topics or Firestore triggers for inter-function communication.

### 5.4. API Endpoints (Suggested)

*   `POST /triggerHarvest`: Initiates lead harvesting.
    *   **Request Body**: `{ "city": "string", "templateId": number, "numLeads": number }`
*   `POST /processLead`: Submits a harvested lead for enrichment and triage.
    *   **Request Body**: `{ "leadData": object, "sourceUrl": "string", "imageHash": "string" }`
*   `POST /queueEmail`: Adds a lead to the email queue.
    *   **Request Body**: `{ "leadData": object, "templateId": number, "city": "string", "timezone": "string" }`
*   `POST /resolveCaptcha`: Receives CAPTCHA resolution results.
    *   **Request Body**: `{ "leadId": "string", "captchaSuccess": boolean, "extractedEmail": "string" }`
*   `GET /getTemplates`: Retrieves available email templates.
    *   **Response Body**: `[ { "templateId": number, "templateName": "string", ... } ]`
*   `POST /authenticateOutlook`: Initiates OAuth flow.
    *   **Response Body**: `{ "authUrl": "string" }`
*   `POST /handleOutlookCallback`: Receives OAuth callback.
    *   **Request Body**: `{ "code": "string", "state": "string" }`
    *   **Response Body**: `{ "message": "string" }`
*   `GET /getConfig`: Retrieves system configurations.
    *   **Response Body**: `{ "llmTransactionLimit": number, "workingHours": object, ... }`
*   `PUT /updateConfig`: Updates system configurations.
    *   **Request Body**: `{ "llmTransactionLimit": number, ... }`

## 6. Data Migration Strategy

### 6.1. Metadata Tables Migration

Scripts will be developed to migrate data from existing SQLite databases to Firestore.

*   **`contacts` Table**: Migrated to Firestore `contacts` collection.
*   **`processor_queue` Table**: Migrated to Firestore `processorQueue` collection.
*   **`templates` Table**: Migrated to Firestore `templates` collection.
*   **`email_queue` Table**: Migrated to Firestore `emailQueue` collection.

### 6.2. Email Templates Migration

*   Functions will extract `base_subject` and `base_body_html` from the `templates` table.
*   These will be stored in Firestore `templates` collection.
*   **Note**: Migration of `variation_storage` is excluded from this initial phase.

## 7. Examples of Interactions (Post-Migration)

### 7.1. `lead_gen_main` to Harvester Function

```http
POST https://us-central1-your-project-id.cloudfunctions.net/triggerHarvest
Content-Type: application/json

{
  "city": "austin",
  "templateId": 1,
  "numLeads": 50
}
```

### 7.2. Processor Function to Email Sender Function (via Firestore Trigger)

*   A new document is written to the Firestore `emailQueue` collection.
    ```json
    // Document in Firestore 'emailQueue' collection
    {
      "templateId": 1,
      "leadData": {
        "phone": "1234567890",
        "name": "John Doe",
        "email": "john.doe@example.com",
        "websiteUrl": "http://example.com",
        "businessName": "Example Corp",
        "category": "Consulting",
        "servicesRendered": ["Consulting", "Training"],
        "status": "PROCESSED",
        "city": "austin"
      },
      "city": "austin",
      "status": "QUEUED",
      "createdAt": "timestamp",
      "timezone": "America/Chicago"
    }
    ```

### 7.3. Processor Function to Contacts Database (Firestore)

```javascript
// Example using Firebase Admin SDK in a Cloud Function
const admin = require('firebase-admin');
admin.initializeApp();
const db = admin.firestore();

async function updateContactStatus(leadId, newStatus, leadData) {
  await db.collection('contacts').doc(leadId).set({
    ...leadData,
    status: newStatus,
    lastUpdated: admin.firestore.FieldValue.serverTimestamp()
  }, { merge: true });
}

// Call example:
// updateContactStatus('some_lead_id', 'PROCESSED', leadData);
```

### 7.4. Client Dashboard to Configuration Management Function

*   **Updating LLM Transaction Limit**:
    ```http
    PUT https://us-central1-your-project-id.cloudfunctions.net/updateConfig
    Content-Type: application/json

    {
      "llmTransactionLimit": 20 // Or any other desired value
    }
    ```

This plan provides a comprehensive overview for migrating the lead generation system to a server-side architecture using Firebase Functions, ensuring all components and configurations are meticulously handled.
