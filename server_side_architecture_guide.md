# Server-Side Architecture Guide: AI-Powered Business Service Automation Platform

## 1. Introduction

This document serves as the definitive guide to the server-side architecture of the AI-Powered Business Service Automation Platform. It details the structure, components, interactions, dependencies, data management, and operational workflows, all designed to be scalable, reliable, and maintainable using Firebase Functions and Firestore. This is a living document intended for continuous updating as the system evolves, reflecting the grand vision of a comprehensive service offering AI agents for SEO, Content Creation, Competitor Analysis, Lead Generation, and more. The Lead Generation Agent Team, detailed herein, is the foundational module within this larger, multi-agent ecosystem.

## 2. Overall Server-Side Architecture Overview

The server-side of the platform is built upon **Google Firebase**, leveraging its serverless capabilities to provide a robust and scalable backend capable of hosting and orchestrating multiple distinct agent teams and services.

*   **Core Services**:
    *   **Firebase Functions**: The primary compute layer, hosting all backend logic as event-driven, serverless functions that represent individual agents or agent team operations.
    *   **Firestore**: The primary NoSQL database for storing all persistent data, including contacts, queues, templates, configurations, authentication tokens, and data specific to various agent teams (e.g., SEO analysis results, social media content).
    *   **Pub/Sub**: Used for asynchronous communication between Firebase Functions, enabling decoupled workflows and event-driven processing across different agent teams.
    *   **Cloud Scheduler**: Used for triggering time-based operations, such as initiating lead harvesting or scheduled SEO analysis tasks.
    *   **Firebase Authentication**: Manages user authentication for the client dashboard and potentially for service accounts interacting with the platform.
    *   **Cloud Storage**: Potentially used for storing larger assets like email attachments, scraped data, or generated content if not directly managed in Firestore.

*   **Interaction Points**:
    *   **Client-Side (`lead_gen_main.py`)**: Interacts with Firebase Functions via HTTP requests for campaign initiation, configuration management, and manual CAPTCHA resolution. This remains the primary client interface for controlling specific agent operations.
    *   **Web Application/Dashboard**: Interacts with Firebase Functions and Firestore for displaying data, managing campaigns, configuring settings for all agent teams, and handling user authentication. This is the central control panel for the entire platform.
    *   **External Services**: Integrates with LLM APIs (via LLM Governor), Microsoft Graph API (for email sending), Google Business APIs, and potentially other third-party services relevant to SEO, Content Creation, and Competitor Analysis.

## 3. Core Server-Side Components (Firebase Functions)

All backend logic is implemented as Firebase Functions, triggered by various events, representing distinct agents or agent team operations.

### 3.1. Lead Generation Agent Team

This team comprises the foundational agents for acquiring and engaging leads.

#### 3.1.1. Harvester Function

*   **Role**: Initiates lead discovery and initial data collection from external sources (e.g., Craigslist). This is the first step in the lead generation pipeline.
*   **Triggers**:
    *   **Cloud Scheduler**: For scheduled, periodic scraping runs (e.g., daily, hourly) to ensure a continuous flow of potential leads.
    *   **HTTP Request**: From `lead_gen_main.py` for on-demand campaign initiation, allowing manual control over lead generation efforts.
*   **Dependencies**:
    *   **Playwright**: For web scraping. Due to potential limitations in direct Cloud Functions execution, this might be deployed within a containerized environment (e.g., Cloud Run) invoked by the function, or a managed scraping service might be used.
    *   **Firestore**: For managing state (harvested URLs, duplicate checks using image hashes), storing raw lead data temporarily in a `rawLeads` collection.
    *   **Pub/Sub**: To publish messages for the Processor Function upon successful harvesting, enabling asynchronous processing.
*   **Logic**:
    *   Performs web scraping of target websites based on client-initiated campaigns.
    *   Generates image hashes for duplicate detection.
    *   Filters out leads with duplicate image hashes or those already processed (checking against Firestore `contacts` collection).
    *   Processes leads in batches to manage resource usage.
    *   Publishes messages containing harvested URL data to a Pub/Sub topic (e.g., `new-harvested-urls`) or writes to a Firestore collection (e.g., `rawLeads`).
*   **Daemon-like Handling**: Achieved through batch processing and the use of Pub/Sub or Firestore triggers to chain operations, simulating continuous background work.

#### 3.1.2. Processor Function

*   **Role**: Enriches raw leads, performs advanced filtering, and triages leads for email processing or manual CAPTCHA resolution. This function acts as the intelligence layer for lead data.
*   **Triggers**:
    *   **Pub/Sub Topic**: `new-harvested-urls` (messages from Harvester Function).
    *   **Firestore Trigger**: Document creation in `rawLeads` collection.
*   **Dependencies**:
    *   **LLM Governor Function**: All LLM calls are routed through this function to manage costs and API limits.
    *   **Firestore**: For reading `contacts`, `templates`, `configurations`, and writing to `contacts`, `emailQueue`, `captchaQueue`.
*   **Logic**:
    *   Scrapes detailed post bodies from provided URLs.
    *   Extracts and normalizes phone numbers.
    *   Performs LLM enrichment (business name, category, services, website scoring) via the LLM Governor. This is where initial data for personalization is gathered.
    *   Conducts duplicate checks against the Firestore `contacts` collection.
    *   Triage logic:
        *   Leads with direct emails are added to the `emailQueue` Firestore collection.
        *   Leads requiring CAPTCHA are added to the `captchaQueue` Firestore collection.
*   **LLM Integration**: Explicitly routes all LLM calls through the LLM Governor Function.
*   **LLM Governor Interaction**: Enforces configurable transaction limits (e.g., 20/min), configurable via the client dashboard's admin section and stored in Firestore `configurations` collection.

#### 3.1.3. Email Sender Function (Email Agent)

*   **Role**: Manages email templates, generates personalized content, and sends emails to leads. This acts as the dedicated "Email Agent," crucial for client outreach.
*   **Triggers**:
    *   **Firestore Trigger**: Document creation in the `emailQueue` collection.
*   **Dependencies**:
    *   **Firestore**: For fetching `templates`, `authTokens`, `configurations`.
    *   **Microsoft Graph API**: For sending emails.
    *   **Firebase Authentication**: For managing user identities and potentially service accounts.
    *   **LLM Governor Function**: Potentially used for dynamic subject line generation or content variation if not pre-generated.
*   **Logic**:
    *   Fetches email templates and variations from Firestore `templates` collection.
    *   Personalizes emails using lead data and templates, leveraging insights gathered by the Processor Function.
    *   Authenticates with Microsoft Graph API using securely stored tokens (managed by the Authentication Management Function).
    *   Sends emails with intervals and pause mechanisms to simulate human sending and avoid account lockout, preserving the sophisticated logic from the original `email-sender.py`.
    *   Updates email status in Firestore `emailQueue` and potentially `contacts`.
    *   **Preservation of Logic**: Meticulously preserves the sophisticated timing and sending logic from the original `email-sender.py`.
    *   **Future Goals**: Designed for extensibility to support Google Business Accounts and other mass email systems.

### 3.2. Other Agent Teams (Future & Planned)

These sections outline planned agents and their roles within the broader platform.

#### 3.2.1. Social Media Agent (Planned)

*   **Role**: To manage social media presence, content posting, and engagement.
*   **Triggers**: Cloud Scheduler, HTTP requests from dashboard, Pub/Sub messages.
*   **Dependencies**: Firestore (for social media accounts, content, schedules), Social Media APIs (Twitter, Facebook, LinkedIn, etc.), LLM Governor (for content generation/optimization).
*   **Logic**: Content scheduling, posting, monitoring engagement, potentially generating social media updates.

#### 3.2.2. Personalization Agent (Critical Future Agent)

*   **Role**: The core agent for creating highly personalized customer experiences across emails and web content. It leverages data from all other agents.
*   **Triggers**: Firestore triggers (e.g., new lead data, user interaction events), HTTP requests.
*   **Dependencies**:
    *   **All other Agent Teams**: Relies heavily on data collected and processed by Lead Generation, Social Media, SEO, and Competitor Analysis agents.
    *   **LLM Governor Function**: For generating highly tailored content.
    *   **Firestore**: For storing user profiles, personalization rules, and generated content.
    *   **Web Application/Dashboard**: To deliver personalized web experiences.
*   **Logic**: Analyzes user data, campaign context, and historical interactions to dynamically tailor email content, subject lines, and potentially website content or user journeys. This agent is critical for achieving "spectacular marketing campaigns."

### 3.3. Supporting Functions

These functions provide essential infrastructure and management capabilities.

#### 3.3.1. CAPTCHA Resolution Orchestrator Function

*   **Role**: Coordinates the manual CAPTCHA resolution process for leads requiring it.
*   **Triggers**: HTTP Request from `lead_gen_main.py`, HTTP Callback from user resolution.
*   **Dependencies**: Firestore (`captchaQueue`, `contacts`), `lead_gen_main.py` (for user interaction).
*   **Logic**: Presents CAPTCHA details, receives resolution, updates lead status, triggers further processing.

#### 3.3.2. Authentication Management Function

*   **Role**: Securely handles OAuth flows for email sending accounts (Microsoft Outlook, Google Business Accounts, etc.).
*   **Triggers**: HTTP Request from `lead_gen_main.py` (initiate OAuth), HTTP Callback from OAuth provider.
*   **Dependencies**: Firebase Authentication, Firestore (`authTokens`).
*   **Logic**: Manages token exchange, secure storage, and refresh mechanisms. Integrates with the client dashboard for user-initiated authentication.

#### 3.3.3. Configuration Management Function

*   **Role**: Manages and serves system-wide configuration settings.
*   **Triggers**: HTTP GET/PUT from client dashboard.
*   **Dependencies**: Firestore (`configurations`).
*   **Logic**: Provides API for reading/writing configurations (LLM limits, working hours, agent-specific settings) accessible by all functions and the dashboard.

#### 3.3.4. LLM Governor Function

*   **Role**: Centralized control for LLM API usage, rate limiting, and cost management across all LLM-dependent agents.
*   **Triggers**: Internal HTTP calls from other Firebase Functions requiring LLM access.
*   **Dependencies**: Firestore (`configurations`), Memorystore (Redis) for real-time rate limiting.
*   **Logic**: Intercepts LLM requests, enforces configurable transaction limits (e.g., 20/min), queues/rejects requests if limits exceeded. Limits are configurable via the client dashboard.

## 4. Data Layer (Firestore Structure)

All data will be managed in Firestore, organized into collections, designed for flexibility across various agent types.

### 4.1. `contacts` Collection

*   **Purpose**: Stores processed lead information, replacing `master_contacts.db`'s `contacts` table. This will be a central repository for all lead data.
*   **Document ID**: Typically a unique identifier for the lead (e.g., derived from phone number + source, or a generated UUID).
*   **Document Structure**:
    ```json
    {
      "phone": "String",
      "name": "String",
      "email": "String",
      "lastSent": "Timestamp",
      "sourceUrl": "String",
      "imageHash": "String",
      "businessName": "String",
      "category": "String",
      "servicesRendered": ["String", "String"],
      "status": "String", // e.g., 'PROCESSED', 'PENDING_CAPTCHA', 'SENT_EMAIL', 'ANALYZED_SEO'
      "city": "String",
      "leadDataJson": { /* Original lead data object */ },
      "agentSpecificData": { // Section for data from other agents
        "seoAnalysis": { /* ... */ },
        "competitorAnalysis": { /* ... */ },
        "personalizationInsights": { /* ... */ }
      },
      "createdAt": "Timestamp",
      "lastUpdated": "Timestamp"
    }
    ```

### 4.2. `processorQueue` Collection

*   **Purpose**: Manages requests for lead processing, replacing `master_contacts.db`'s `processor_queue` table.
*   **Document ID**: Auto-generated by Firestore.
*   **Document Structure**:
    ```json
    {
      "templateId": "Number",
      "city": "String",
      "numberOfLeadsToProcess": "Number",
      "requestTimestamp": "Timestamp",
      "status": "String", // e.g., 'PENDING', 'PROCESSING', 'COMPLETED', 'FAILED'
      "processedAt": "Timestamp"
    }
    ```

### 4.3. `templates` Collection

*   **Purpose**: Stores base email templates, replacing `email_engine.db`'s `templates` table.
*   **Document ID**: Auto-generated by Firestore, or can use `templateId` from SQLite.
*   **Document Structure**:
    ```json
    {
      "templateId": "Number", // Optional if using auto-generated IDs
      "templateName": "String",
      "description": "String",
      "baseSubject": "String",
      "baseBodyHtml": "String",
      "isArchived": "Boolean",
      "createdAt": "Timestamp"
    }
    ```

### 4.4. `emailQueue` Collection

*   **Purpose**: Holds leads ready to be emailed, replacing `email_engine.db`'s `email_queue` table.
*   **Document ID**: Auto-generated by Firestore.
*   **Document Structure**:
    ```json
    {
      "templateId": "Number",
      "leadData": { /* Lead data object */ },
      "city": "String",
      "status": "String", // e.g., 'QUEUED', 'SENT', 'ERROR_NO_EMAIL', 'ERROR_API_FAILURE'
      "createdAt": "Timestamp",
      "sentAt": "Timestamp",
      "timezone": "String"
    }
    ```

### 4.5. `authTokens` Collection

*   **Purpose**: Securely stores OAuth tokens for email sending accounts.
*   **Document ID**: Typically related to the user or service account (e.g., `userId` or `serviceAccountIdentifier`).
*   **Document Structure**:
    ```json
    {
      "userId": "String",
      "provider": "String", // e.g., 'microsoft', 'google'
      "accessToken": "String", // Encrypted access token
      "refreshToken": "String", // Encrypted refresh token
      "expiresAt": "Timestamp",
      "scopes": ["String", "String"],
      "createdAt": "Timestamp",
      "lastRefreshed": "Timestamp"
    }
    ```

### 4.6. `configurations` Collection

*   **Purpose**: Stores system-wide configuration settings for all agents.
*   **Document ID**: A single document, e.g., `systemSettings`.
*   **Document Structure**:
    ```json
    {
      "llmTransactionLimitPerMinute": "Number", // e.g., 20
      "workingHours": { /* ... */ },
      "targetTimezones": ["String", "String"],
      "emailProviderSettings": { /* ... */ },
      "llmGovernorSettings": { /* ... */ },
      "agentSpecificConfigs": { // Section for agent-specific settings
        "leadGeneration": { /* ... */ },
        "socialMedia": { /* ... */ },
        "personalization": { /* ... */ }
      }
    }
    ```

### 4.7. `rawLeads` Collection (Temporary)

*   **Purpose**: Temporarily stores raw lead data harvested before processing.
*   **Document ID**: Auto-generated by Firestore.
*   **Document Structure**:
    ```json
    {
      "url": "String",
      "post_id": "String",
      "image_hash": "String",
      "original_category": "String",
      "harvestedAt": "Timestamp"
    }
    ```

### 4.8. `captchaQueue` Collection

*   **Purpose**: Stores leads that require manual CAPTCHA resolution.
*   **Document ID**: Auto-generated by Firestore.
*   **Document Structure**:
    ```json
    {
      "leadId": "String", // Reference to the 'contacts' document
      "leadData": { /* Lead data object */ },
      "status": "String", // e.g., 'PENDING_USER_ACTION', 'RESOLVED', 'FAILED'
      "createdAt": "Timestamp",
      "resolutionUrl": "String" // URL for user to resolve CAPTCHA
    }
    ```

### 4.9. `socialMediaPosts` Collection (Future)

*   **Purpose**: Stores scheduled and posted social media content.
*   **Document ID**: Auto-generated by Firestore.
*   **Document Structure**:
    ```json
    {
      "agent": "String", // e.g., "SocialMediaAgent"
      "platform": "String", // e.g., "Twitter", "LinkedIn"
      "content": "String",
      "mediaUrls": ["String"],
      "scheduleTime": "Timestamp",
      "status": "String", // e.g., 'SCHEDULED', 'POSTED', 'FAILED'
      "postedAt": "Timestamp",
      "engagementMetrics": { /* ... */ }
    }
    ```

### 4.10. `personalizationData` Collection (Future)

*   **Purpose**: Stores data and insights used by the Personalization Agent.
*   **Document ID**: User/Lead ID.
*   **Document Structure**:
    ```json
    {
      "userId": "String",
      "preferences": { /* ... */ },
      "interactionHistory": [ /* ... */ ],
      "personalizationScores": { /* ... */ },
      "generatedContentSnippets": { /* ... */ }
    }
    ```

## 5. Inter-Component Communication & Workflows

This section details how server-side components (agents) interact with each other and with the client.

### 5.1. Campaign Initiation Workflow (Lead Generation)

1.  **Client (`lead_gen_main.py`)**:
    *   User selects campaign parameters (city, template, lead count).
    *   `lead_gen_main.py` makes an HTTP POST request to the `triggerHarvest` endpoint of the Harvester Function.
    *   **Request**: `{ "city": "austin", "templateId": 1, "numLeads": 50 }`

2.  **Harvester Function**:
    *   Receives the request.
    *   Initiates web scraping.
    *   Stores harvested raw lead data in Firestore `rawLeads`.
    *   Publishes messages to `new-harvested-urls` Pub/Sub topic.

3.  **Processor Function**:
    *   Triggered by `new-harvested-urls` Pub/Sub messages.
    *   Fetches raw lead data, scrapes details, performs LLM enrichment (via LLM Governor), and checks duplicates.
    *   Triage leads: Writes to `emailQueue` or `captchaQueue` Firestore collections.

### 5.2. Email Sending Workflow (Email Agent)

1.  **Processor Function**: Writes leads to Firestore `emailQueue`.
2.  **Email Sender Function (Email Agent)**:
    *   Triggered by new documents in `emailQueue`.
    *   Fetches lead data, templates, and auth tokens from Firestore.
    *   Personalizes email content.
    *   Sends email via Microsoft Graph API.
    *   Updates status in `emailQueue` and `contacts`.

### 5.3. CAPTCHA Resolution Workflow

1.  **Processor Function**: Identifies leads needing CAPTCHA, writes to Firestore `captchaQueue`.
2.  **`lead_gen_main.py` (Client)**:
    *   Polls status or listens for Firestore changes related to `captchaQueue`.
    *   Calls `resolveCaptcha` endpoint of CAPTCHA Orchestrator Function.
    *   **Request**: `{ "leadId": "...", "captchaSuccess": true/false, "extractedEmail": "..." }`
3.  **CAPTCHA Resolution Orchestrator Function**:
    *   Receives resolution details.
    *   Updates `captchaQueue` and `contacts` in Firestore.
    *   Triggers Processor Function (via Pub/Sub or Firestore write) upon successful resolution.

### 5.4. Authentication Workflow

1.  **`lead_gen_main.py` (Client)**:
    *   User initiates Outlook authentication via dashboard.
    *   Calls `authenticateOutlook` endpoint of Authentication Management Function.
    *   **Response**: `{ "authUrl": "https://login.microsoftonline.com/..." }`
    *   Opens URL for user authorization.
2.  **Authentication Management Function**:
    *   Receives OAuth callback, exchanges code for tokens.
    *   Securely stores refresh tokens in Firestore `authTokens`.
    *   Notifies client of success/failure.

### 5.5. Configuration Management Workflow

1.  **Client Dashboard**:
    *   Admin user accesses dashboard to modify configurations.
    *   Makes HTTP GET/PUT requests to `getConfig`/`updateConfig` on Configuration Management Function.
2.  **Configuration Management Function**:
    *   Reads/writes to Firestore `configurations`.
    *   Other functions (LLM Governor, Email Sender) read settings from Firestore.

### 5.6. Inter-Agent Dependencies & Phased Rollout

*   **Phase 1: Foundation**:
    *   **Harvester & Processor Agents**: Must be fully functional and tested. They provide the raw data and initial processing necessary for other agents.
    *   **Email Sender Agent**: Must be functional to handle the output of the Processor.
*   **Phase 2: Supporting Agents**:
    *   **Social Media Agent**: Can be developed and tested once the core data pipeline is stable. Its output might feed into the Personalization Agent.
    *   **SEO/Competitor Analysis Agents**: Can be developed in parallel or sequentially, feeding insights into `contacts` or dedicated analysis collections.
*   **Phase 3: Personalization Agent**:
    *   This agent is highly dependent on the data and insights gathered by all preceding agents.
    *   It will consume data from `contacts` (lead data, processed info), `templates` (for email structure), and potentially other agent-specific collections (e.g., `personalizationData`, `socialMediaPosts`) to craft highly personalized emails and web experiences.
    *   Its successful implementation relies on the robust functioning of the Harvester, Processor, and Email Sender, as well as the availability of rich data.

## 6. Existing Server-Side System Integration

*   **Integration with Existing Backend**: Firebase Functions expose RESTful APIs or use Pub/Sub for communication with existing backend services. Data is exchanged in JSON format.
*   **Google Conversational Agents**:
    *   **Integration Strategy**: Firebase Functions act as webhooks for Google Assistant or Dialogflow.
    *   **Example**: A Dialogflow agent can trigger a Firebase Function to query Firestore for campaign status or lead details, returning information to the user via the conversational interface.
    *   **Data Flow**: Functions can push data to Google services or receive data from them, facilitating a connected experience.

## 7. Overall Web Application (Dashboard) Interaction

*   **Purpose**: The central control panel for managing all agent teams, monitoring processes, configuring settings, and handling user authentication.
*   **Interaction**:
    *   The dashboard (e.g., React/Vue/Angular app on Firebase Hosting) uses Firebase SDK or HTTP requests to Firebase Functions.
    *   Reads data from Firestore to display campaign status, lead details, agent performance, and configurations.
    *   Initiates actions (start campaigns, resolve CAPTCHAs, manage agents) by calling Firebase Functions.
    *   Manages user authentication via Firebase Authentication.
    *   Admin section interacts with Configuration Management Function to update system settings for all agents.

## 8. Future Goals & Enhancements

*   **Agent Team Expansion**:
    *   Develop and integrate **Social Media Agent** for content scheduling and posting.
    *   Develop and integrate **SEO Agent** for website analysis and reporting.
    *   Develop and integrate **Competitor Analysis Agent** for market insights.
    *   **Crucially, develop and integrate the Personalization Agent** to leverage data from all other agents for hyper-personalized customer outreach and web experiences, enabling "spectacular marketing campaigns."
*   **Email Provider Extensibility**: Design the Email Sender Function for easy integration with other mass email systems (SendGrid, Mailgun).
*   **Google Business Accounts Integration**: Implement OAuth and sending logic for Google Business Accounts as an alternative email sending source.
*   **Automated CAPTCHA Resolution**: Explore services for automated CAPTCHA solving.
*   **Advanced Analytics**: Implement detailed logging and analytics for agent performance, LLM usage, and campaign effectiveness.
*   **LLM Capabilities Expansion**: Enhance LLM usage for more sophisticated lead qualification, personalized outreach, and automated response generation across all agents.
*   **Variation Storage Migration**: Migrate `variationStorage` from SQLite to Firestore.
*   **Real-time Monitoring**: Implement real-time dashboards using Firestore listeners or WebSockets.

This document provides a comprehensive blueprint for the server-side architecture of your AI-Powered Business Service Automation Platform, emphasizing the phased rollout of agents and the critical role of personalization.
