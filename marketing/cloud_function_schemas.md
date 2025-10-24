# Cloud Function Schemas

## `receiveLeadData` Function

**Description**: Receives lead data via an HTTP POST request and saves it to Firestore.

**Parameters**:
*   `phone`: Normalized phone number (e.g., +1XXXXXXXXXX). (Required)
*   `raw_contact`: Raw contact data. (Required)
*   `name`: Contact's name. (Optional, Defaults to null)
*   `email`: Contact's email address. (Optional, Defaults to null)
*   `last_sent`: Status of last sent. (Optional, Defaults to 'PENDING')
*   `source_url`: The URL from which the lead originated. (Optional, Defaults to null)
*   `image_hash`: An image hash. (Optional, Defaults to null)
*   `business_name`: The lead's business name. (Optional, Defaults to null)
*   `category`: The lead's category. (Optional, Defaults to null)
*   `services_rendered`: Services rendered. (Optional, Defaults to null)

**Response (Success)**:
*   `success`: Indicates if the operation was successful.
*   `message`: A message describing the outcome.
*   `contactId`: The unique ID generated for the contact.
*   `hash`: The unique 4-alphanumeric hash generated for the contact.
*   `phone`: The normalized phone number.

**Errors**:
*   `status`: 405, `message`: "Method Not Allowed. Only POST requests are accepted."
*   `status`: 400, `message`: "Missing required parameter: phone or raw_contact"
*   `status`: 400, `message`: "Invalid phone number format provided."
*   `status`: 500, `message`: "Error saving lead data: [error message]"
*   `status`: 500, `message`: "An unknown error occurred while saving lead data."

---

## `addLeadToSMSQueue` Function

**Description**: Callable function to add a lead to the SMS queue.

**Parameters**:
*   `leadId`: The Firestore lead ID (from saveLeadToServer). (Required)
*   `phone`: The phone number to be normalized and added (+1XXXXXXXXXX). (Required)
*   `campaignId`: ID of the campaign (e.g., 99 = SMS campaign). (Required)

**Response (Success)**:
*   `success`: Indicates if the operation was successful.
*   `id`: The ID of the newly created document in the smsQueue collection.
*   `leadId`: The Firestore lead ID.
*   `phone`: The normalized phone number.
*   `campaignId`: The campaign ID.
*   `status`: The status of the queue entry, typically 'pending'.
*   `createdAt`: Server timestamp of creation.

**Errors**:
*   `code`: "invalid-argument", `message`: "leadId, phone, and campaignId are required"
*   `code`: "invalid-argument", `message`: "Phone number must be 10 or 11 digits"
*   `code`: "internal", `message`: "Unknown error"
