// =============================================================================
// AUTO-GENERATED - do not edit by hand.
// Source: FastAPI OpenAPI schema. Regenerate with:
//   python scripts/export_openapi.py
// =============================================================================

export const apiSchema = {
  "openapi": "3.1.0",
  "info": {
    "title": "SentinelFlow - Fraud Detection API",
    "description": "\n## SentinelFlow Real-Time Fraud Detection Platform\n\n**TEKNOFEST 2026 Finans Teknolojileri** yarışması için geliştirilmiş,\nyapay zeka destekli dolandırıcılık tespit platformu.\n\n### Özellikler\n- **ML Ensemble**: IsolationForest + XGBoost + AutoEncoder ile çoklu model oylama\n- **PostgreSQL**: Kalıcı alert ve case yönetimi\n- **Case Management**: Alert korelasyonu, triage, audit log\n- **WebSocket**: Canlı alert akışı\n- **Explainability**: Neden dolandırıcılık tespit edildiğini açıklar\n\n### API Grupları\n- `/api/v1/alerts` - Alarm listesi ve detayları\n- `/api/v1/cases` - Vaka yönetimi\n- `/api/v1/transactions` - İşlem analizi\n- `/api/v1/system` - Sistem sağlık ve istatistikler\n- `/ws/alerts` - WebSocket canlı alert akışı\n    ",
    "version": "2.1.0"
  },
  "paths": {
    "/api/v1/auth/login": {
      "post": {
        "tags": [
          "Authentication"
        ],
        "summary": "User login",
        "description": "Authenticate with username/email and password to receive JWT tokens.",
        "operationId": "login_api_v1_auth_login_post",
        "requestBody": {
          "content": {
            "application/json": {
              "schema": {
                "$ref": "#/components/schemas/LoginRequest"
              }
            }
          },
          "required": true
        },
        "responses": {
          "200": {
            "description": "Successful Response",
            "content": {
              "application/json": {
                "schema": {
                  "$ref": "#/components/schemas/TokenResponse"
                }
              }
            }
          },
          "422": {
            "description": "Validation Error",
            "content": {
              "application/json": {
                "schema": {
                  "$ref": "#/components/schemas/HTTPValidationError"
                }
              }
            }
          }
        }
      }
    },
    "/api/v1/auth/logout": {
      "post": {
        "tags": [
          "Authentication"
        ],
        "summary": "User logout",
        "description": "Revoke refresh tokens to logout user.",
        "operationId": "logout_api_v1_auth_logout_post",
        "security": [
          {
            "HTTPBearer": []
          }
        ],
        "parameters": [
          {
            "name": "refresh_token",
            "in": "query",
            "required": false,
            "schema": {
              "anyOf": [
                {
                  "type": "string"
                },
                {
                  "type": "null"
                }
              ],
              "title": "Refresh Token"
            }
          }
        ],
        "responses": {
          "200": {
            "description": "Successful Response",
            "content": {
              "application/json": {
                "schema": {}
              }
            }
          },
          "422": {
            "description": "Validation Error",
            "content": {
              "application/json": {
                "schema": {
                  "$ref": "#/components/schemas/HTTPValidationError"
                }
              }
            }
          }
        }
      }
    },
    "/api/v1/auth/register": {
      "post": {
        "tags": [
          "Authentication"
        ],
        "summary": "Register new user",
        "description": "Create a new user account. Default role is 'viewer'.",
        "operationId": "register_api_v1_auth_register_post",
        "requestBody": {
          "content": {
            "application/json": {
              "schema": {
                "$ref": "#/components/schemas/UserCreate"
              }
            }
          },
          "required": true
        },
        "responses": {
          "200": {
            "description": "Successful Response",
            "content": {
              "application/json": {
                "schema": {
                  "$ref": "#/components/schemas/UserPublic"
                }
              }
            }
          },
          "422": {
            "description": "Validation Error",
            "content": {
              "application/json": {
                "schema": {
                  "$ref": "#/components/schemas/HTTPValidationError"
                }
              }
            }
          }
        }
      }
    },
    "/api/v1/auth/refresh": {
      "post": {
        "tags": [
          "Authentication"
        ],
        "summary": "Refresh access token",
        "description": "Get new access token using refresh token.",
        "operationId": "refresh_tokens_api_v1_auth_refresh_post",
        "requestBody": {
          "content": {
            "application/json": {
              "schema": {
                "$ref": "#/components/schemas/RefreshRequest"
              }
            }
          },
          "required": true
        },
        "responses": {
          "200": {
            "description": "Successful Response",
            "content": {
              "application/json": {
                "schema": {
                  "$ref": "#/components/schemas/TokenResponse"
                }
              }
            }
          },
          "422": {
            "description": "Validation Error",
            "content": {
              "application/json": {
                "schema": {
                  "$ref": "#/components/schemas/HTTPValidationError"
                }
              }
            }
          }
        }
      }
    },
    "/api/v1/auth/me": {
      "get": {
        "tags": [
          "Authentication"
        ],
        "summary": "Get current user",
        "description": "Returns the currently authenticated user's profile.",
        "operationId": "get_me_api_v1_auth_me_get",
        "responses": {
          "200": {
            "description": "Successful Response",
            "content": {
              "application/json": {
                "schema": {
                  "$ref": "#/components/schemas/User"
                }
              }
            }
          }
        },
        "security": [
          {
            "HTTPBearer": []
          }
        ]
      }
    },
    "/api/v1/auth/change-password": {
      "post": {
        "tags": [
          "Authentication"
        ],
        "summary": "Change password",
        "description": "Change the current user's password.",
        "operationId": "change_password_api_v1_auth_change_password_post",
        "requestBody": {
          "content": {
            "application/json": {
              "schema": {
                "$ref": "#/components/schemas/PasswordChangeRequest"
              }
            }
          },
          "required": true
        },
        "responses": {
          "200": {
            "description": "Successful Response",
            "content": {
              "application/json": {
                "schema": {}
              }
            }
          },
          "422": {
            "description": "Validation Error",
            "content": {
              "application/json": {
                "schema": {
                  "$ref": "#/components/schemas/HTTPValidationError"
                }
              }
            }
          }
        },
        "security": [
          {
            "HTTPBearer": []
          }
        ]
      }
    },
    "/api/v1/alerts": {
      "get": {
        "tags": [
          "Alerts"
        ],
        "summary": "List fraud alerts",
        "description": "Returns a paginated list of fraud alerts from the database.",
        "operationId": "list_alerts_api_v1_alerts_get",
        "security": [
          {
            "HTTPBearer": []
          }
        ],
        "parameters": [
          {
            "name": "page",
            "in": "query",
            "required": false,
            "schema": {
              "type": "integer",
              "minimum": 1,
              "description": "Page number",
              "default": 1,
              "title": "Page"
            },
            "description": "Page number"
          },
          {
            "name": "page_size",
            "in": "query",
            "required": false,
            "schema": {
              "type": "integer",
              "maximum": 100,
              "minimum": 1,
              "description": "Items per page",
              "default": 20,
              "title": "Page Size"
            },
            "description": "Items per page"
          },
          {
            "name": "fraud_type",
            "in": "query",
            "required": false,
            "schema": {
              "anyOf": [
                {
                  "type": "string"
                },
                {
                  "type": "null"
                }
              ],
              "description": "Filter by fraud type",
              "title": "Fraud Type"
            },
            "description": "Filter by fraud type"
          },
          {
            "name": "severity",
            "in": "query",
            "required": false,
            "schema": {
              "anyOf": [
                {
                  "type": "string"
                },
                {
                  "type": "null"
                }
              ],
              "description": "Filter by severity",
              "title": "Severity"
            },
            "description": "Filter by severity"
          },
          {
            "name": "is_dismissed",
            "in": "query",
            "required": false,
            "schema": {
              "anyOf": [
                {
                  "type": "boolean"
                },
                {
                  "type": "null"
                }
              ],
              "description": "Filter by dismissed status",
              "title": "Is Dismissed"
            },
            "description": "Filter by dismissed status"
          },
          {
            "name": "start_date",
            "in": "query",
            "required": false,
            "schema": {
              "anyOf": [
                {
                  "type": "string",
                  "format": "date-time"
                },
                {
                  "type": "null"
                }
              ],
              "description": "Filter by start date",
              "title": "Start Date"
            },
            "description": "Filter by start date"
          },
          {
            "name": "end_date",
            "in": "query",
            "required": false,
            "schema": {
              "anyOf": [
                {
                  "type": "string",
                  "format": "date-time"
                },
                {
                  "type": "null"
                }
              ],
              "description": "Filter by end date",
              "title": "End Date"
            },
            "description": "Filter by end date"
          },
          {
            "name": "sender_iban",
            "in": "query",
            "required": false,
            "schema": {
              "anyOf": [
                {
                  "type": "string"
                },
                {
                  "type": "null"
                }
              ],
              "description": "Filter by sender IBAN",
              "title": "Sender Iban"
            },
            "description": "Filter by sender IBAN"
          },
          {
            "name": "receiver_iban",
            "in": "query",
            "required": false,
            "schema": {
              "anyOf": [
                {
                  "type": "string"
                },
                {
                  "type": "null"
                }
              ],
              "description": "Filter by receiver IBAN",
              "title": "Receiver Iban"
            },
            "description": "Filter by receiver IBAN"
          }
        ],
        "responses": {
          "200": {
            "description": "Successful Response",
            "content": {
              "application/json": {
                "schema": {
                  "$ref": "#/components/schemas/AlertListResponse"
                }
              }
            }
          },
          "422": {
            "description": "Validation Error",
            "content": {
              "application/json": {
                "schema": {
                  "$ref": "#/components/schemas/HTTPValidationError"
                }
              }
            }
          }
        }
      }
    },
    "/api/v1/alerts/stats": {
      "get": {
        "tags": [
          "Alerts"
        ],
        "summary": "Get alert statistics",
        "description": "Returns aggregate statistics for alerts.",
        "operationId": "get_alert_stats_api_v1_alerts_stats_get",
        "security": [
          {
            "HTTPBearer": []
          }
        ],
        "parameters": [
          {
            "name": "start_date",
            "in": "query",
            "required": false,
            "schema": {
              "anyOf": [
                {
                  "type": "string",
                  "format": "date-time"
                },
                {
                  "type": "null"
                }
              ],
              "title": "Start Date"
            }
          },
          {
            "name": "end_date",
            "in": "query",
            "required": false,
            "schema": {
              "anyOf": [
                {
                  "type": "string",
                  "format": "date-time"
                },
                {
                  "type": "null"
                }
              ],
              "title": "End Date"
            }
          }
        ],
        "responses": {
          "200": {
            "description": "Successful Response",
            "content": {
              "application/json": {
                "schema": {}
              }
            }
          },
          "422": {
            "description": "Validation Error",
            "content": {
              "application/json": {
                "schema": {
                  "$ref": "#/components/schemas/HTTPValidationError"
                }
              }
            }
          }
        }
      }
    },
    "/api/v1/alerts/{alert_id}": {
      "get": {
        "tags": [
          "Alerts"
        ],
        "summary": "Get alert details",
        "description": "Returns details for a specific alert.",
        "operationId": "get_alert_api_v1_alerts__alert_id__get",
        "security": [
          {
            "HTTPBearer": []
          }
        ],
        "parameters": [
          {
            "name": "alert_id",
            "in": "path",
            "required": true,
            "schema": {
              "type": "string",
              "title": "Alert Id"
            }
          }
        ],
        "responses": {
          "200": {
            "description": "Successful Response",
            "content": {
              "application/json": {
                "schema": {
                  "$ref": "#/components/schemas/Alert"
                }
              }
            }
          },
          "422": {
            "description": "Validation Error",
            "content": {
              "application/json": {
                "schema": {
                  "$ref": "#/components/schemas/HTTPValidationError"
                }
              }
            }
          }
        }
      }
    },
    "/api/v1/alerts/{alert_id}/dismiss": {
      "post": {
        "tags": [
          "Alerts"
        ],
        "summary": "Dismiss an alert",
        "description": "Mark an alert as dismissed (false positive).",
        "operationId": "dismiss_alert_api_v1_alerts__alert_id__dismiss_post",
        "security": [
          {
            "HTTPBearer": []
          }
        ],
        "parameters": [
          {
            "name": "alert_id",
            "in": "path",
            "required": true,
            "schema": {
              "type": "string",
              "title": "Alert Id"
            }
          },
          {
            "name": "reason",
            "in": "query",
            "required": false,
            "schema": {
              "type": "string",
              "description": "Dismissal reason",
              "title": "Reason"
            },
            "description": "Dismissal reason"
          }
        ],
        "responses": {
          "200": {
            "description": "Successful Response",
            "content": {
              "application/json": {
                "schema": {
                  "$ref": "#/components/schemas/Alert"
                }
              }
            }
          },
          "422": {
            "description": "Validation Error",
            "content": {
              "application/json": {
                "schema": {
                  "$ref": "#/components/schemas/HTTPValidationError"
                }
              }
            }
          }
        }
      }
    },
    "/api/v1/alerts/{alert_id}/link-case/{case_id}": {
      "post": {
        "tags": [
          "Alerts"
        ],
        "summary": "Link alert to case",
        "description": "Link an alert to an existing case.",
        "operationId": "link_alert_to_case_api_v1_alerts__alert_id__link_case__case_id__post",
        "security": [
          {
            "HTTPBearer": []
          }
        ],
        "parameters": [
          {
            "name": "alert_id",
            "in": "path",
            "required": true,
            "schema": {
              "type": "string",
              "title": "Alert Id"
            }
          },
          {
            "name": "case_id",
            "in": "path",
            "required": true,
            "schema": {
              "type": "string",
              "title": "Case Id"
            }
          }
        ],
        "responses": {
          "200": {
            "description": "Successful Response",
            "content": {
              "application/json": {
                "schema": {}
              }
            }
          },
          "422": {
            "description": "Validation Error",
            "content": {
              "application/json": {
                "schema": {
                  "$ref": "#/components/schemas/HTTPValidationError"
                }
              }
            }
          }
        }
      }
    },
    "/api/v1/cases": {
      "get": {
        "tags": [
          "Cases"
        ],
        "summary": "List cases",
        "description": "Returns a paginated list of investigation cases.",
        "operationId": "list_cases_api_v1_cases_get",
        "security": [
          {
            "HTTPBearer": []
          }
        ],
        "parameters": [
          {
            "name": "page",
            "in": "query",
            "required": false,
            "schema": {
              "type": "integer",
              "minimum": 1,
              "default": 1,
              "title": "Page"
            }
          },
          {
            "name": "page_size",
            "in": "query",
            "required": false,
            "schema": {
              "type": "integer",
              "maximum": 100,
              "minimum": 1,
              "default": 20,
              "title": "Page Size"
            }
          },
          {
            "name": "status",
            "in": "query",
            "required": false,
            "schema": {
              "anyOf": [
                {
                  "type": "string"
                },
                {
                  "type": "null"
                }
              ],
              "description": "Filter by status",
              "title": "Status"
            },
            "description": "Filter by status"
          },
          {
            "name": "priority",
            "in": "query",
            "required": false,
            "schema": {
              "anyOf": [
                {
                  "type": "string"
                },
                {
                  "type": "null"
                }
              ],
              "description": "Filter by priority",
              "title": "Priority"
            },
            "description": "Filter by priority"
          },
          {
            "name": "assigned_to",
            "in": "query",
            "required": false,
            "schema": {
              "anyOf": [
                {
                  "type": "string"
                },
                {
                  "type": "null"
                }
              ],
              "description": "Filter by assignee",
              "title": "Assigned To"
            },
            "description": "Filter by assignee"
          },
          {
            "name": "is_open",
            "in": "query",
            "required": false,
            "schema": {
              "anyOf": [
                {
                  "type": "boolean"
                },
                {
                  "type": "null"
                }
              ],
              "description": "Filter open/closed cases",
              "title": "Is Open"
            },
            "description": "Filter open/closed cases"
          }
        ],
        "responses": {
          "200": {
            "description": "Successful Response",
            "content": {
              "application/json": {
                "schema": {
                  "$ref": "#/components/schemas/CaseListResponse"
                }
              }
            }
          },
          "422": {
            "description": "Validation Error",
            "content": {
              "application/json": {
                "schema": {
                  "$ref": "#/components/schemas/HTTPValidationError"
                }
              }
            }
          }
        }
      },
      "post": {
        "tags": [
          "Cases"
        ],
        "summary": "Create a new case",
        "description": "Create a new investigation case from alerts.",
        "operationId": "create_case_api_v1_cases_post",
        "security": [
          {
            "HTTPBearer": []
          }
        ],
        "requestBody": {
          "required": true,
          "content": {
            "application/json": {
              "schema": {
                "$ref": "#/components/schemas/CaseCreate"
              }
            }
          }
        },
        "responses": {
          "200": {
            "description": "Successful Response",
            "content": {
              "application/json": {
                "schema": {
                  "$ref": "#/components/schemas/Case"
                }
              }
            }
          },
          "422": {
            "description": "Validation Error",
            "content": {
              "application/json": {
                "schema": {
                  "$ref": "#/components/schemas/HTTPValidationError"
                }
              }
            }
          }
        }
      }
    },
    "/api/v1/cases/stats": {
      "get": {
        "tags": [
          "Cases"
        ],
        "summary": "Get case statistics",
        "description": "Get case statistics.",
        "operationId": "get_case_stats_api_v1_cases_stats_get",
        "responses": {
          "200": {
            "description": "Successful Response",
            "content": {
              "application/json": {
                "schema": {}
              }
            }
          }
        },
        "security": [
          {
            "HTTPBearer": []
          }
        ]
      }
    },
    "/api/v1/cases/{case_id}": {
      "get": {
        "tags": [
          "Cases"
        ],
        "summary": "Get case details",
        "description": "Get a specific case by ID.",
        "operationId": "get_case_api_v1_cases__case_id__get",
        "security": [
          {
            "HTTPBearer": []
          }
        ],
        "parameters": [
          {
            "name": "case_id",
            "in": "path",
            "required": true,
            "schema": {
              "type": "string",
              "title": "Case Id"
            }
          }
        ],
        "responses": {
          "200": {
            "description": "Successful Response",
            "content": {
              "application/json": {
                "schema": {
                  "$ref": "#/components/schemas/Case"
                }
              }
            }
          },
          "422": {
            "description": "Validation Error",
            "content": {
              "application/json": {
                "schema": {
                  "$ref": "#/components/schemas/HTTPValidationError"
                }
              }
            }
          }
        }
      },
      "patch": {
        "tags": [
          "Cases"
        ],
        "summary": "Update case",
        "description": "Update case fields.",
        "operationId": "update_case_api_v1_cases__case_id__patch",
        "security": [
          {
            "HTTPBearer": []
          }
        ],
        "parameters": [
          {
            "name": "case_id",
            "in": "path",
            "required": true,
            "schema": {
              "type": "string",
              "title": "Case Id"
            }
          }
        ],
        "requestBody": {
          "required": true,
          "content": {
            "application/json": {
              "schema": {
                "$ref": "#/components/schemas/CaseUpdate"
              }
            }
          }
        },
        "responses": {
          "200": {
            "description": "Successful Response",
            "content": {
              "application/json": {
                "schema": {
                  "$ref": "#/components/schemas/Case"
                }
              }
            }
          },
          "422": {
            "description": "Validation Error",
            "content": {
              "application/json": {
                "schema": {
                  "$ref": "#/components/schemas/HTTPValidationError"
                }
              }
            }
          }
        }
      }
    },
    "/api/v1/cases/{case_id}/assign": {
      "post": {
        "tags": [
          "Cases"
        ],
        "summary": "Assign case",
        "description": "Assign case to analyst/team.",
        "operationId": "assign_case_api_v1_cases__case_id__assign_post",
        "security": [
          {
            "HTTPBearer": []
          }
        ],
        "parameters": [
          {
            "name": "case_id",
            "in": "path",
            "required": true,
            "schema": {
              "type": "string",
              "title": "Case Id"
            }
          },
          {
            "name": "assigned_to",
            "in": "query",
            "required": true,
            "schema": {
              "type": "string",
              "description": "Username to assign to",
              "title": "Assigned To"
            },
            "description": "Username to assign to"
          },
          {
            "name": "assigned_team",
            "in": "query",
            "required": false,
            "schema": {
              "anyOf": [
                {
                  "type": "string"
                },
                {
                  "type": "null"
                }
              ],
              "description": "Team name",
              "title": "Assigned Team"
            },
            "description": "Team name"
          }
        ],
        "responses": {
          "200": {
            "description": "Successful Response",
            "content": {
              "application/json": {
                "schema": {
                  "$ref": "#/components/schemas/Case"
                }
              }
            }
          },
          "422": {
            "description": "Validation Error",
            "content": {
              "application/json": {
                "schema": {
                  "$ref": "#/components/schemas/HTTPValidationError"
                }
              }
            }
          }
        }
      }
    },
    "/api/v1/cases/{case_id}/add-alert/{alert_id}": {
      "post": {
        "tags": [
          "Cases"
        ],
        "summary": "Add alert to case",
        "description": "Add an alert to an existing case.",
        "operationId": "add_alert_to_case_api_v1_cases__case_id__add_alert__alert_id__post",
        "security": [
          {
            "HTTPBearer": []
          }
        ],
        "parameters": [
          {
            "name": "case_id",
            "in": "path",
            "required": true,
            "schema": {
              "type": "string",
              "title": "Case Id"
            }
          },
          {
            "name": "alert_id",
            "in": "path",
            "required": true,
            "schema": {
              "type": "string",
              "title": "Alert Id"
            }
          }
        ],
        "responses": {
          "200": {
            "description": "Successful Response",
            "content": {
              "application/json": {
                "schema": {}
              }
            }
          },
          "422": {
            "description": "Validation Error",
            "content": {
              "application/json": {
                "schema": {
                  "$ref": "#/components/schemas/HTTPValidationError"
                }
              }
            }
          }
        }
      }
    },
    "/api/v1/cases/{case_id}/events": {
      "get": {
        "tags": [
          "Cases"
        ],
        "summary": "Get case events (audit log)",
        "description": "Get audit log events for a case.",
        "operationId": "get_case_events_api_v1_cases__case_id__events_get",
        "security": [
          {
            "HTTPBearer": []
          }
        ],
        "parameters": [
          {
            "name": "case_id",
            "in": "path",
            "required": true,
            "schema": {
              "type": "string",
              "title": "Case Id"
            }
          },
          {
            "name": "page",
            "in": "query",
            "required": false,
            "schema": {
              "type": "integer",
              "minimum": 1,
              "default": 1,
              "title": "Page"
            }
          },
          {
            "name": "page_size",
            "in": "query",
            "required": false,
            "schema": {
              "type": "integer",
              "maximum": 100,
              "minimum": 1,
              "default": 50,
              "title": "Page Size"
            }
          },
          {
            "name": "event_type",
            "in": "query",
            "required": false,
            "schema": {
              "anyOf": [
                {
                  "type": "string"
                },
                {
                  "type": "null"
                }
              ],
              "title": "Event Type"
            }
          }
        ],
        "responses": {
          "200": {
            "description": "Successful Response",
            "content": {
              "application/json": {
                "schema": {}
              }
            }
          },
          "422": {
            "description": "Validation Error",
            "content": {
              "application/json": {
                "schema": {
                  "$ref": "#/components/schemas/HTTPValidationError"
                }
              }
            }
          }
        }
      }
    },
    "/api/v1/ml/models": {
      "get": {
        "tags": [
          "Machine Learning"
        ],
        "summary": "Get ML model status",
        "description": "Returns the status of all ML models in the ensemble.",
        "operationId": "get_model_status_api_v1_ml_models_get",
        "responses": {
          "200": {
            "description": "Successful Response",
            "content": {
              "application/json": {
                "schema": {
                  "$ref": "#/components/schemas/ModelStatusResponse"
                }
              }
            }
          }
        },
        "security": [
          {
            "HTTPBearer": []
          }
        ]
      }
    },
    "/api/v1/ml/train": {
      "post": {
        "tags": [
          "Machine Learning"
        ],
        "summary": "Train ML models",
        "description": "Trigger training of all ML models with synthetic data.",
        "operationId": "train_models_api_v1_ml_train_post",
        "requestBody": {
          "content": {
            "application/json": {
              "schema": {
                "$ref": "#/components/schemas/TrainRequest"
              }
            }
          },
          "required": true
        },
        "responses": {
          "200": {
            "description": "Successful Response",
            "content": {
              "application/json": {
                "schema": {
                  "$ref": "#/components/schemas/TrainResponse"
                }
              }
            }
          },
          "422": {
            "description": "Validation Error",
            "content": {
              "application/json": {
                "schema": {
                  "$ref": "#/components/schemas/HTTPValidationError"
                }
              }
            }
          }
        },
        "security": [
          {
            "HTTPBearer": []
          }
        ]
      }
    },
    "/api/v1/ml/train/status": {
      "get": {
        "tags": [
          "Machine Learning"
        ],
        "summary": "Get training status",
        "description": "Get current training status.",
        "operationId": "get_training_status_api_v1_ml_train_status_get",
        "responses": {
          "200": {
            "description": "Successful Response",
            "content": {
              "application/json": {
                "schema": {}
              }
            }
          }
        },
        "security": [
          {
            "HTTPBearer": []
          }
        ]
      }
    },
    "/api/v1/ml/features": {
      "get": {
        "tags": [
          "Machine Learning"
        ],
        "summary": "Get feature definitions",
        "description": "Returns the list of features used by the ML ensemble.",
        "operationId": "get_features_api_v1_ml_features_get",
        "responses": {
          "200": {
            "description": "Successful Response",
            "content": {
              "application/json": {
                "schema": {}
              }
            }
          }
        },
        "security": [
          {
            "HTTPBearer": []
          }
        ]
      }
    },
    "/api/v1/graph/data": {
      "get": {
        "tags": [
          "Graph"
        ],
        "summary": "Get Graph Data",
        "description": "Get transaction network graph data for visualization.\n\nReturns nodes (accounts) and links (transactions) for force-directed graph.",
        "operationId": "get_graph_data_api_v1_graph_data_get",
        "security": [
          {
            "HTTPBearer": []
          }
        ],
        "parameters": [
          {
            "name": "limit",
            "in": "query",
            "required": false,
            "schema": {
              "type": "integer",
              "maximum": 1000,
              "minimum": 1,
              "default": 100,
              "title": "Limit"
            }
          },
          {
            "name": "hours",
            "in": "query",
            "required": false,
            "schema": {
              "type": "integer",
              "maximum": 168,
              "minimum": 1,
              "default": 24,
              "title": "Hours"
            }
          },
          {
            "name": "include_fraud_only",
            "in": "query",
            "required": false,
            "schema": {
              "type": "boolean",
              "default": false,
              "title": "Include Fraud Only"
            }
          }
        ],
        "responses": {
          "200": {
            "description": "Successful Response",
            "content": {
              "application/json": {
                "schema": {
                  "$ref": "#/components/schemas/GraphData"
                }
              }
            }
          },
          "422": {
            "description": "Validation Error",
            "content": {
              "application/json": {
                "schema": {
                  "$ref": "#/components/schemas/HTTPValidationError"
                }
              }
            }
          }
        }
      }
    },
    "/api/v1/graph/rings": {
      "get": {
        "tags": [
          "Graph"
        ],
        "summary": "Get Fraud Rings",
        "description": "Get detected fraud rings (circular transaction patterns).",
        "operationId": "get_fraud_rings_api_v1_graph_rings_get",
        "security": [
          {
            "HTTPBearer": []
          }
        ],
        "parameters": [
          {
            "name": "min_depth",
            "in": "query",
            "required": false,
            "schema": {
              "type": "integer",
              "maximum": 10,
              "minimum": 2,
              "default": 3,
              "title": "Min Depth"
            }
          },
          {
            "name": "max_depth",
            "in": "query",
            "required": false,
            "schema": {
              "type": "integer",
              "maximum": 10,
              "minimum": 3,
              "default": 6,
              "title": "Max Depth"
            }
          },
          {
            "name": "limit",
            "in": "query",
            "required": false,
            "schema": {
              "type": "integer",
              "maximum": 50,
              "minimum": 1,
              "default": 10,
              "title": "Limit"
            }
          }
        ],
        "responses": {
          "200": {
            "description": "Successful Response",
            "content": {
              "application/json": {
                "schema": {
                  "type": "array",
                  "items": {
                    "$ref": "#/components/schemas/FraudRing"
                  },
                  "title": "Response Get Fraud Rings Api V1 Graph Rings Get"
                }
              }
            }
          },
          "422": {
            "description": "Validation Error",
            "content": {
              "application/json": {
                "schema": {
                  "$ref": "#/components/schemas/HTTPValidationError"
                }
              }
            }
          }
        }
      }
    },
    "/api/v1/graph/account/{iban}": {
      "get": {
        "tags": [
          "Graph"
        ],
        "summary": "Get Account Network",
        "description": "Get transaction network centered on a specific account.",
        "operationId": "get_account_network_api_v1_graph_account__iban__get",
        "security": [
          {
            "HTTPBearer": []
          }
        ],
        "parameters": [
          {
            "name": "iban",
            "in": "path",
            "required": true,
            "schema": {
              "type": "string",
              "title": "Iban"
            }
          },
          {
            "name": "depth",
            "in": "query",
            "required": false,
            "schema": {
              "type": "integer",
              "maximum": 4,
              "minimum": 1,
              "default": 2,
              "title": "Depth"
            }
          }
        ],
        "responses": {
          "200": {
            "description": "Successful Response",
            "content": {
              "application/json": {
                "schema": {
                  "$ref": "#/components/schemas/GraphData"
                }
              }
            }
          },
          "422": {
            "description": "Validation Error",
            "content": {
              "application/json": {
                "schema": {
                  "$ref": "#/components/schemas/HTTPValidationError"
                }
              }
            }
          }
        }
      }
    },
    "/api/v1/chat/": {
      "post": {
        "tags": [
          "Chat"
        ],
        "summary": "Chat",
        "description": "Process a chat message and return AI response.\n\nThis is a rule-based chatbot for fraud analysis assistance.\nFor production, integrate with LLM APIs.",
        "operationId": "chat_api_v1_chat__post",
        "requestBody": {
          "content": {
            "application/json": {
              "schema": {
                "$ref": "#/components/schemas/ChatMessage"
              }
            }
          },
          "required": true
        },
        "responses": {
          "200": {
            "description": "Successful Response",
            "content": {
              "application/json": {
                "schema": {
                  "$ref": "#/components/schemas/ChatResponse"
                }
              }
            }
          },
          "422": {
            "description": "Validation Error",
            "content": {
              "application/json": {
                "schema": {
                  "$ref": "#/components/schemas/HTTPValidationError"
                }
              }
            }
          }
        },
        "security": [
          {
            "HTTPBearer": []
          }
        ]
      }
    },
    "/api/v1/chat": {
      "post": {
        "tags": [
          "Chat"
        ],
        "summary": "Chat",
        "description": "Process a chat message and return AI response.\n\nThis is a rule-based chatbot for fraud analysis assistance.\nFor production, integrate with LLM APIs.",
        "operationId": "chat_api_v1_chat_post",
        "requestBody": {
          "content": {
            "application/json": {
              "schema": {
                "$ref": "#/components/schemas/ChatMessage"
              }
            }
          },
          "required": true
        },
        "responses": {
          "200": {
            "description": "Successful Response",
            "content": {
              "application/json": {
                "schema": {
                  "$ref": "#/components/schemas/ChatResponse"
                }
              }
            }
          },
          "422": {
            "description": "Validation Error",
            "content": {
              "application/json": {
                "schema": {
                  "$ref": "#/components/schemas/HTTPValidationError"
                }
              }
            }
          }
        },
        "security": [
          {
            "HTTPBearer": []
          }
        ]
      }
    },
    "/api/v1/chat/suggestions": {
      "get": {
        "tags": [
          "Chat"
        ],
        "summary": "Get Suggestions",
        "description": "Get suggested questions for the chat interface.",
        "operationId": "get_suggestions_api_v1_chat_suggestions_get",
        "responses": {
          "200": {
            "description": "Successful Response",
            "content": {
              "application/json": {
                "schema": {
                  "items": {
                    "type": "string"
                  },
                  "type": "array",
                  "title": "Response Get Suggestions Api V1 Chat Suggestions Get"
                }
              }
            }
          }
        },
        "security": [
          {
            "HTTPBearer": []
          }
        ]
      }
    },
    "/api/v1/chat/knowledge/{fraud_type}": {
      "get": {
        "tags": [
          "Chat"
        ],
        "summary": "Get Fraud Knowledge",
        "description": "Get detailed knowledge about a specific fraud type.",
        "operationId": "get_fraud_knowledge_api_v1_chat_knowledge__fraud_type__get",
        "security": [
          {
            "HTTPBearer": []
          }
        ],
        "parameters": [
          {
            "name": "fraud_type",
            "in": "path",
            "required": true,
            "schema": {
              "type": "string",
              "title": "Fraud Type"
            }
          }
        ],
        "responses": {
          "200": {
            "description": "Successful Response",
            "content": {
              "application/json": {
                "schema": {
                  "type": "object",
                  "additionalProperties": true,
                  "title": "Response Get Fraud Knowledge Api V1 Chat Knowledge  Fraud Type  Get"
                }
              }
            }
          },
          "422": {
            "description": "Validation Error",
            "content": {
              "application/json": {
                "schema": {
                  "$ref": "#/components/schemas/HTTPValidationError"
                }
              }
            }
          }
        }
      }
    },
    "/api/v1/kyc/screen": {
      "post": {
        "tags": [
          "KYC & Compliance"
        ],
        "summary": "Screen a customer (PEP & Sanctions)",
        "description": "Performs combined PEP and sanctions screening on a customer name and returns matches, an aggregate risk score, and a recommendation.",
        "operationId": "screen_customer_api_v1_kyc_screen_post",
        "requestBody": {
          "content": {
            "application/json": {
              "schema": {
                "$ref": "#/components/schemas/KYCScreenRequest"
              }
            }
          },
          "required": true
        },
        "responses": {
          "200": {
            "description": "Successful Response",
            "content": {
              "application/json": {
                "schema": {
                  "$ref": "#/components/schemas/KYCScreenResponse"
                }
              }
            }
          },
          "422": {
            "description": "Validation Error",
            "content": {
              "application/json": {
                "schema": {
                  "$ref": "#/components/schemas/HTTPValidationError"
                }
              }
            }
          }
        },
        "security": [
          {
            "HTTPBearer": []
          }
        ]
      }
    },
    "/api/v1/risk/score": {
      "post": {
        "tags": [
          "Risk Scoring"
        ],
        "summary": "Real-time risk scoring",
        "description": "Score a single transaction for fraud risk. Target latency: <30ms",
        "operationId": "score_transaction_api_v1_risk_score_post",
        "requestBody": {
          "content": {
            "application/json": {
              "schema": {
                "$ref": "#/components/schemas/RiskScoringRequest"
              }
            }
          },
          "required": true
        },
        "responses": {
          "200": {
            "description": "Successful Response",
            "content": {
              "application/json": {
                "schema": {
                  "$ref": "#/components/schemas/RiskScoringResponse"
                }
              }
            }
          },
          "422": {
            "description": "Validation Error",
            "content": {
              "application/json": {
                "schema": {
                  "$ref": "#/components/schemas/HTTPValidationError"
                }
              }
            }
          }
        },
        "security": [
          {
            "HTTPBearer": []
          }
        ]
      }
    },
    "/api/v1/risk/batch": {
      "post": {
        "tags": [
          "Risk Scoring"
        ],
        "summary": "Batch risk scoring",
        "description": "Score multiple transactions in parallel",
        "operationId": "score_batch_api_v1_risk_batch_post",
        "requestBody": {
          "content": {
            "application/json": {
              "schema": {
                "$ref": "#/components/schemas/BatchRiskRequest"
              }
            }
          },
          "required": true
        },
        "responses": {
          "200": {
            "description": "Successful Response",
            "content": {
              "application/json": {
                "schema": {
                  "$ref": "#/components/schemas/BatchRiskResponse"
                }
              }
            }
          },
          "422": {
            "description": "Validation Error",
            "content": {
              "application/json": {
                "schema": {
                  "$ref": "#/components/schemas/HTTPValidationError"
                }
              }
            }
          }
        },
        "security": [
          {
            "HTTPBearer": []
          }
        ]
      }
    },
    "/api/v1/risk/stats": {
      "get": {
        "tags": [
          "Risk Scoring"
        ],
        "summary": "Risk scoring statistics",
        "description": "Get risk scoring engine statistics.",
        "operationId": "get_stats_api_v1_risk_stats_get",
        "responses": {
          "200": {
            "description": "Successful Response",
            "content": {
              "application/json": {
                "schema": {
                  "additionalProperties": true,
                  "type": "object",
                  "title": "Response Get Stats Api V1 Risk Stats Get"
                }
              }
            }
          }
        },
        "security": [
          {
            "HTTPBearer": []
          }
        ]
      }
    },
    "/api/v1/risk/features": {
      "get": {
        "tags": [
          "Risk Scoring"
        ],
        "summary": "List available features",
        "description": "List all available features and their descriptions.",
        "operationId": "list_features_api_v1_risk_features_get",
        "responses": {
          "200": {
            "description": "Successful Response",
            "content": {
              "application/json": {
                "schema": {
                  "additionalProperties": true,
                  "type": "object",
                  "title": "Response List Features Api V1 Risk Features Get"
                }
              }
            }
          }
        },
        "security": [
          {
            "HTTPBearer": []
          }
        ]
      }
    },
    "/api/v1/transactions": {
      "post": {
        "tags": [
          "Transactions"
        ],
        "summary": "Submit a transaction for fraud analysis",
        "description": "Analyze a transaction for fraud.",
        "operationId": "submit_transaction_api_v1_transactions_post",
        "requestBody": {
          "content": {
            "application/json": {
              "schema": {
                "$ref": "#/components/schemas/TransactionCreate"
              }
            }
          },
          "required": true
        },
        "responses": {
          "200": {
            "description": "Successful Response",
            "content": {
              "application/json": {
                "schema": {
                  "$ref": "#/components/schemas/TransactionResponse"
                }
              }
            }
          },
          "422": {
            "description": "Validation Error",
            "content": {
              "application/json": {
                "schema": {
                  "$ref": "#/components/schemas/HTTPValidationError"
                }
              }
            }
          }
        },
        "security": [
          {
            "HTTPBearer": []
          }
        ]
      }
    },
    "/api/v1/system/health": {
      "get": {
        "tags": [
          "System"
        ],
        "summary": "Health check",
        "description": "Check system health.",
        "operationId": "health_check_api_v1_system_health_get",
        "responses": {
          "200": {
            "description": "Successful Response",
            "content": {
              "application/json": {
                "schema": {
                  "$ref": "#/components/schemas/HealthResponse"
                }
              }
            }
          }
        }
      }
    },
    "/api/v1/system/stats": {
      "get": {
        "tags": [
          "System"
        ],
        "summary": "System statistics",
        "description": "Get system-wide statistics.",
        "operationId": "system_stats_api_v1_system_stats_get",
        "responses": {
          "200": {
            "description": "Successful Response",
            "content": {
              "application/json": {
                "schema": {
                  "$ref": "#/components/schemas/StatsResponse"
                }
              }
            }
          }
        }
      }
    },
    "/": {
      "get": {
        "tags": [
          "Root"
        ],
        "summary": "Root",
        "description": "API root - service info.",
        "operationId": "root__get",
        "responses": {
          "200": {
            "description": "Successful Response",
            "content": {
              "application/json": {
                "schema": {}
              }
            }
          }
        }
      }
    },
    "/metrics": {
      "get": {
        "tags": [
          "Monitoring"
        ],
        "summary": "Metrics",
        "description": "Prometheus-compatible metrics endpoint.",
        "operationId": "metrics_metrics_get",
        "responses": {
          "200": {
            "description": "Successful Response",
            "content": {
              "application/json": {
                "schema": {}
              }
            }
          }
        }
      }
    }
  },
  "components": {
    "schemas": {
      "Alert": {
        "properties": {
          "alert_id": {
            "type": "string",
            "title": "Alert Id"
          },
          "fraud_type": {
            "$ref": "#/components/schemas/FraudType"
          },
          "severity": {
            "$ref": "#/components/schemas/Severity"
          },
          "confidence": {
            "type": "number",
            "maximum": 1.0,
            "minimum": 0.0,
            "title": "Confidence"
          },
          "transaction_id": {
            "type": "string",
            "title": "Transaction Id"
          },
          "sender_iban": {
            "type": "string",
            "title": "Sender Iban"
          },
          "sender_name": {
            "type": "string",
            "title": "Sender Name"
          },
          "sender_city": {
            "type": "string",
            "title": "Sender City",
            "default": ""
          },
          "receiver_iban": {
            "type": "string",
            "title": "Receiver Iban"
          },
          "receiver_name": {
            "type": "string",
            "title": "Receiver Name"
          },
          "receiver_city": {
            "type": "string",
            "title": "Receiver City",
            "default": ""
          },
          "amount": {
            "type": "number",
            "title": "Amount"
          },
          "currency": {
            "type": "string",
            "title": "Currency",
            "default": "TRY"
          },
          "title": {
            "type": "string",
            "title": "Title",
            "default": ""
          },
          "description": {
            "type": "string",
            "title": "Description",
            "default": ""
          },
          "evidence": {
            "items": {
              "$ref": "#/components/schemas/Evidence"
            },
            "type": "array",
            "title": "Evidence"
          },
          "related_transactions": {
            "items": {
              "type": "string"
            },
            "type": "array",
            "title": "Related Transactions"
          },
          "related_accounts": {
            "items": {
              "type": "string"
            },
            "type": "array",
            "title": "Related Accounts"
          },
          "case_id": {
            "anyOf": [
              {
                "type": "string"
              },
              {
                "type": "null"
              }
            ],
            "title": "Case Id",
            "description": "Linked case ID (if correlated)"
          },
          "detected_at": {
            "type": "string",
            "format": "date-time",
            "title": "Detected At"
          },
          "updated_at": {
            "type": "string",
            "format": "date-time",
            "title": "Updated At"
          },
          "is_dismissed": {
            "type": "boolean",
            "title": "Is Dismissed",
            "description": "Manually dismissed by analyst",
            "default": false
          },
          "dismissed_by": {
            "anyOf": [
              {
                "type": "string"
              },
              {
                "type": "null"
              }
            ],
            "title": "Dismissed By"
          },
          "dismissed_at": {
            "anyOf": [
              {
                "type": "string",
                "format": "date-time"
              },
              {
                "type": "null"
              }
            ],
            "title": "Dismissed At"
          },
          "dismissed_reason": {
            "anyOf": [
              {
                "type": "string"
              },
              {
                "type": "null"
              }
            ],
            "title": "Dismissed Reason"
          },
          "detector_versions": {
            "additionalProperties": {
              "type": "string"
            },
            "type": "object",
            "title": "Detector Versions",
            "description": "Version of each detector that contributed"
          },
          "processing_time_ms": {
            "type": "number",
            "title": "Processing Time Ms",
            "default": 0.0
          }
        },
        "type": "object",
        "required": [
          "fraud_type",
          "severity",
          "confidence",
          "transaction_id",
          "sender_iban",
          "sender_name",
          "receiver_iban",
          "receiver_name",
          "amount"
        ],
        "title": "Alert",
        "description": "Full alert record with database fields."
      },
      "AlertListResponse": {
        "properties": {
          "total": {
            "type": "integer",
            "title": "Total",
            "description": "Total number of matching alerts"
          },
          "page": {
            "type": "integer",
            "minimum": 1.0,
            "title": "Page",
            "default": 1
          },
          "page_size": {
            "type": "integer",
            "maximum": 100.0,
            "minimum": 1.0,
            "title": "Page Size",
            "default": 20
          },
          "alerts": {
            "items": {
              "$ref": "#/components/schemas/Alert"
            },
            "type": "array",
            "title": "Alerts"
          },
          "filters": {
            "additionalProperties": true,
            "type": "object",
            "title": "Filters"
          }
        },
        "type": "object",
        "required": [
          "total"
        ],
        "title": "AlertListResponse",
        "description": "Paginated list of alerts."
      },
      "BatchRiskRequest": {
        "properties": {
          "transactions": {
            "items": {
              "$ref": "#/components/schemas/RiskScoringRequest"
            },
            "type": "array",
            "title": "Transactions"
          },
          "parallel": {
            "type": "boolean",
            "title": "Parallel",
            "description": "Paralel işleme",
            "default": true
          }
        },
        "type": "object",
        "required": [
          "transactions"
        ],
        "title": "BatchRiskRequest",
        "description": "Toplu risk skorlama isteği."
      },
      "BatchRiskResponse": {
        "properties": {
          "total": {
            "type": "integer",
            "title": "Total"
          },
          "processed": {
            "type": "integer",
            "title": "Processed"
          },
          "avg_latency_ms": {
            "type": "number",
            "title": "Avg Latency Ms"
          },
          "high_risk_count": {
            "type": "integer",
            "title": "High Risk Count"
          },
          "results": {
            "items": {
              "$ref": "#/components/schemas/RiskScoringResponse"
            },
            "type": "array",
            "title": "Results"
          }
        },
        "type": "object",
        "required": [
          "total",
          "processed",
          "avg_latency_ms",
          "high_risk_count",
          "results"
        ],
        "title": "BatchRiskResponse",
        "description": "Toplu risk skorlama yanıtı."
      },
      "Case": {
        "properties": {
          "case_id": {
            "type": "string",
            "title": "Case Id"
          },
          "title": {
            "type": "string",
            "title": "Title"
          },
          "description": {
            "type": "string",
            "title": "Description",
            "default": ""
          },
          "status": {
            "$ref": "#/components/schemas/CaseStatus",
            "default": "new"
          },
          "priority": {
            "$ref": "#/components/schemas/CasePriority",
            "default": "P3"
          },
          "primary_fraud_type": {
            "anyOf": [
              {
                "$ref": "#/components/schemas/FraudType"
              },
              {
                "type": "null"
              }
            ]
          },
          "fraud_types": {
            "items": {
              "$ref": "#/components/schemas/FraudType"
            },
            "type": "array",
            "title": "Fraud Types",
            "description": "All fraud types from linked alerts"
          },
          "alert_ids": {
            "items": {
              "type": "string"
            },
            "type": "array",
            "title": "Alert Ids"
          },
          "alert_count": {
            "type": "integer",
            "title": "Alert Count",
            "default": 0
          },
          "total_amount": {
            "type": "number",
            "title": "Total Amount",
            "description": "Sum of transaction amounts",
            "default": 0.0
          },
          "max_severity": {
            "$ref": "#/components/schemas/Severity",
            "default": "medium"
          },
          "avg_confidence": {
            "type": "number",
            "maximum": 1.0,
            "minimum": 0.0,
            "title": "Avg Confidence",
            "default": 0.0
          },
          "involved_accounts": {
            "items": {
              "type": "string"
            },
            "type": "array",
            "title": "Involved Accounts"
          },
          "involved_transactions": {
            "items": {
              "type": "string"
            },
            "type": "array",
            "title": "Involved Transactions"
          },
          "assigned_to": {
            "anyOf": [
              {
                "type": "string"
              },
              {
                "type": "null"
              }
            ],
            "title": "Assigned To"
          },
          "assigned_team": {
            "anyOf": [
              {
                "type": "string"
              },
              {
                "type": "null"
              }
            ],
            "title": "Assigned Team"
          },
          "tags": {
            "items": {
              "type": "string"
            },
            "type": "array",
            "title": "Tags"
          },
          "created_at": {
            "type": "string",
            "format": "date-time",
            "title": "Created At"
          },
          "updated_at": {
            "type": "string",
            "format": "date-time",
            "title": "Updated At"
          },
          "first_alert_at": {
            "anyOf": [
              {
                "type": "string",
                "format": "date-time"
              },
              {
                "type": "null"
              }
            ],
            "title": "First Alert At"
          },
          "last_alert_at": {
            "anyOf": [
              {
                "type": "string",
                "format": "date-time"
              },
              {
                "type": "null"
              }
            ],
            "title": "Last Alert At"
          },
          "sla_due_at": {
            "anyOf": [
              {
                "type": "string",
                "format": "date-time"
              },
              {
                "type": "null"
              }
            ],
            "title": "Sla Due At"
          },
          "sla_breached": {
            "type": "boolean",
            "title": "Sla Breached",
            "default": false
          },
          "resolution": {
            "anyOf": [
              {
                "type": "string"
              },
              {
                "type": "null"
              }
            ],
            "title": "Resolution",
            "description": "Resolution summary"
          },
          "resolved_at": {
            "anyOf": [
              {
                "type": "string",
                "format": "date-time"
              },
              {
                "type": "null"
              }
            ],
            "title": "Resolved At"
          },
          "resolved_by": {
            "anyOf": [
              {
                "type": "string"
              },
              {
                "type": "null"
              }
            ],
            "title": "Resolved By"
          },
          "str_required": {
            "type": "boolean",
            "title": "Str Required",
            "description": "STR filing required",
            "default": false
          },
          "str_filed": {
            "type": "boolean",
            "title": "Str Filed",
            "default": false
          },
          "str_filed_at": {
            "anyOf": [
              {
                "type": "string",
                "format": "date-time"
              },
              {
                "type": "null"
              }
            ],
            "title": "Str Filed At"
          },
          "str_reference": {
            "anyOf": [
              {
                "type": "string"
              },
              {
                "type": "null"
              }
            ],
            "title": "Str Reference"
          },
          "notes_count": {
            "type": "integer",
            "title": "Notes Count",
            "default": 0
          },
          "last_note": {
            "anyOf": [
              {
                "type": "string"
              },
              {
                "type": "null"
              }
            ],
            "title": "Last Note"
          }
        },
        "type": "object",
        "required": [
          "title"
        ],
        "title": "Case",
        "description": "Full case record with database fields."
      },
      "CaseCreate": {
        "properties": {
          "title": {
            "type": "string",
            "maxLength": 300,
            "minLength": 1,
            "title": "Title"
          },
          "description": {
            "type": "string",
            "maxLength": 5000,
            "title": "Description",
            "default": ""
          },
          "alert_ids": {
            "items": {
              "type": "string"
            },
            "type": "array",
            "minItems": 1,
            "title": "Alert Ids"
          },
          "priority": {
            "$ref": "#/components/schemas/CasePriority",
            "default": "P3"
          },
          "primary_fraud_type": {
            "anyOf": [
              {
                "$ref": "#/components/schemas/FraudType"
              },
              {
                "type": "null"
              }
            ]
          },
          "tags": {
            "items": {
              "type": "string"
            },
            "type": "array",
            "title": "Tags"
          },
          "assigned_to": {
            "anyOf": [
              {
                "type": "string"
              },
              {
                "type": "null"
              }
            ],
            "title": "Assigned To",
            "description": "Username of assigned analyst"
          },
          "assigned_team": {
            "anyOf": [
              {
                "type": "string"
              },
              {
                "type": "null"
              }
            ],
            "title": "Assigned Team",
            "description": "Team name"
          }
        },
        "type": "object",
        "required": [
          "title"
        ],
        "title": "CaseCreate",
        "description": "Schema for creating a new case.\nCases are created from correlated alerts."
      },
      "CaseListResponse": {
        "properties": {
          "total": {
            "type": "integer",
            "title": "Total"
          },
          "page": {
            "type": "integer",
            "title": "Page",
            "default": 1
          },
          "page_size": {
            "type": "integer",
            "title": "Page Size",
            "default": 20
          },
          "cases": {
            "items": {
              "$ref": "#/components/schemas/Case"
            },
            "type": "array",
            "title": "Cases"
          },
          "filters": {
            "additionalProperties": true,
            "type": "object",
            "title": "Filters"
          }
        },
        "type": "object",
        "required": [
          "total"
        ],
        "title": "CaseListResponse",
        "description": "Paginated list of cases."
      },
      "CasePriority": {
        "type": "string",
        "enum": [
          "P1",
          "P2",
          "P3",
          "P4"
        ],
        "title": "CasePriority",
        "description": "Case priority levels."
      },
      "CaseStatus": {
        "type": "string",
        "enum": [
          "new",
          "triage",
          "investigating",
          "escalated",
          "pending_info",
          "resolved_true_positive",
          "resolved_false_positive",
          "closed"
        ],
        "title": "CaseStatus",
        "description": "Case lifecycle statuses."
      },
      "CaseUpdate": {
        "properties": {
          "status": {
            "anyOf": [
              {
                "$ref": "#/components/schemas/CaseStatus"
              },
              {
                "type": "null"
              }
            ]
          },
          "priority": {
            "anyOf": [
              {
                "$ref": "#/components/schemas/CasePriority"
              },
              {
                "type": "null"
              }
            ]
          },
          "assigned_to": {
            "anyOf": [
              {
                "type": "string"
              },
              {
                "type": "null"
              }
            ],
            "title": "Assigned To"
          },
          "assigned_team": {
            "anyOf": [
              {
                "type": "string"
              },
              {
                "type": "null"
              }
            ],
            "title": "Assigned Team"
          },
          "tags": {
            "anyOf": [
              {
                "items": {
                  "type": "string"
                },
                "type": "array"
              },
              {
                "type": "null"
              }
            ],
            "title": "Tags"
          },
          "resolution": {
            "anyOf": [
              {
                "type": "string"
              },
              {
                "type": "null"
              }
            ],
            "title": "Resolution"
          },
          "note": {
            "anyOf": [
              {
                "type": "string",
                "maxLength": 5000
              },
              {
                "type": "null"
              }
            ],
            "title": "Note"
          }
        },
        "type": "object",
        "title": "CaseUpdate",
        "description": "Schema for updating a case."
      },
      "ChatMessage": {
        "properties": {
          "message": {
            "type": "string",
            "maxLength": 1000,
            "minLength": 1,
            "title": "Message"
          },
          "context": {
            "anyOf": [
              {
                "additionalProperties": true,
                "type": "object"
              },
              {
                "type": "null"
              }
            ],
            "title": "Context"
          },
          "session_id": {
            "anyOf": [
              {
                "type": "string"
              },
              {
                "type": "null"
              }
            ],
            "title": "Session Id"
          }
        },
        "type": "object",
        "required": [
          "message"
        ],
        "title": "ChatMessage",
        "description": "User chat message."
      },
      "ChatResponse": {
        "properties": {
          "response": {
            "type": "string",
            "title": "Response"
          },
          "suggestions": {
            "items": {
              "type": "string"
            },
            "type": "array",
            "title": "Suggestions"
          },
          "sources": {
            "items": {
              "type": "string"
            },
            "type": "array",
            "title": "Sources"
          },
          "confidence": {
            "type": "number",
            "title": "Confidence",
            "default": 0.9
          },
          "timestamp": {
            "type": "string",
            "title": "Timestamp"
          }
        },
        "type": "object",
        "required": [
          "response"
        ],
        "title": "ChatResponse",
        "description": "AI chat response."
      },
      "ComponentStatus": {
        "properties": {
          "name": {
            "type": "string",
            "title": "Name"
          },
          "status": {
            "type": "string",
            "title": "Status",
            "default": "unknown"
          },
          "latency_ms": {
            "anyOf": [
              {
                "type": "number"
              },
              {
                "type": "null"
              }
            ],
            "title": "Latency Ms"
          },
          "message": {
            "anyOf": [
              {
                "type": "string"
              },
              {
                "type": "null"
              }
            ],
            "title": "Message"
          }
        },
        "type": "object",
        "required": [
          "name"
        ],
        "title": "ComponentStatus",
        "description": "Status of a system component."
      },
      "DetectorType": {
        "type": "string",
        "enum": [
          "rule_engine",
          "graph_analysis",
          "geo_analysis",
          "nlp_analysis",
          "ml_ensemble",
          "compliance_engine"
        ],
        "title": "DetectorType",
        "description": "Types of detection engines."
      },
      "Evidence": {
        "properties": {
          "detector_type": {
            "$ref": "#/components/schemas/DetectorType",
            "description": "Detection engine that produced this evidence"
          },
          "detector_version": {
            "type": "string",
            "title": "Detector Version",
            "default": "1.0.0"
          },
          "rule_id": {
            "anyOf": [
              {
                "type": "string"
              },
              {
                "type": "null"
              }
            ],
            "title": "Rule Id",
            "description": "Rule ID if rule-based"
          },
          "pattern_name": {
            "anyOf": [
              {
                "type": "string"
              },
              {
                "type": "null"
              }
            ],
            "title": "Pattern Name",
            "description": "Pattern name (e.g., 'circular_ring_3_hop')"
          },
          "confidence": {
            "type": "number",
            "maximum": 1.0,
            "minimum": 0.0,
            "title": "Confidence",
            "description": "Detection confidence",
            "default": 0.0
          },
          "contribution": {
            "type": "number",
            "maximum": 1.0,
            "minimum": 0.0,
            "title": "Contribution",
            "description": "Contribution to final score",
            "default": 0.0
          },
          "details": {
            "additionalProperties": true,
            "type": "object",
            "title": "Details",
            "description": "Detector-specific evidence details"
          },
          "summary": {
            "type": "string",
            "title": "Summary",
            "description": "Human-readable explanation",
            "default": ""
          },
          "related_entities": {
            "items": {
              "type": "string"
            },
            "type": "array",
            "title": "Related Entities",
            "description": "Related accounts/entities (IBANs, names)"
          },
          "related_transactions": {
            "items": {
              "type": "string"
            },
            "type": "array",
            "title": "Related Transactions",
            "description": "Related transaction IDs"
          }
        },
        "type": "object",
        "required": [
          "detector_type"
        ],
        "title": "Evidence",
        "description": "Evidence supporting a fraud detection.\nStructured for explainability and audit."
      },
      "FraudRing": {
        "properties": {
          "ring_id": {
            "type": "string",
            "title": "Ring Id"
          },
          "accounts": {
            "items": {
              "type": "string"
            },
            "type": "array",
            "title": "Accounts"
          },
          "total_amount": {
            "type": "number",
            "title": "Total Amount"
          },
          "transaction_count": {
            "type": "integer",
            "title": "Transaction Count"
          },
          "detected_at": {
            "type": "string",
            "title": "Detected At"
          },
          "severity": {
            "type": "string",
            "title": "Severity",
            "default": "high"
          }
        },
        "type": "object",
        "required": [
          "ring_id",
          "accounts",
          "total_amount",
          "transaction_count",
          "detected_at"
        ],
        "title": "FraudRing",
        "description": "Detected fraud ring."
      },
      "FraudType": {
        "type": "string",
        "enum": [
          "circular_ring",
          "impossible_travel",
          "blacklist_keyword",
          "mule_account",
          "structuring",
          "velocity_anomaly",
          "ml_ensemble",
          "compliance_violation"
        ],
        "title": "FraudType",
        "description": "Types of fraud detected by the system."
      },
      "GraphData": {
        "properties": {
          "nodes": {
            "items": {
              "$ref": "#/components/schemas/GraphNode"
            },
            "type": "array",
            "title": "Nodes"
          },
          "links": {
            "items": {
              "$ref": "#/components/schemas/GraphEdge"
            },
            "type": "array",
            "title": "Links"
          },
          "metadata": {
            "additionalProperties": true,
            "type": "object",
            "title": "Metadata"
          }
        },
        "type": "object",
        "required": [
          "nodes",
          "links"
        ],
        "title": "GraphData",
        "description": "Complete graph data for visualization."
      },
      "GraphEdge": {
        "properties": {
          "source": {
            "type": "string",
            "title": "Source"
          },
          "target": {
            "type": "string",
            "title": "Target"
          },
          "amount": {
            "type": "number",
            "title": "Amount"
          },
          "timestamp": {
            "type": "string",
            "title": "Timestamp"
          },
          "color": {
            "anyOf": [
              {
                "type": "string"
              },
              {
                "type": "null"
              }
            ],
            "title": "Color"
          },
          "is_fraud": {
            "type": "boolean",
            "title": "Is Fraud",
            "default": false
          }
        },
        "type": "object",
        "required": [
          "source",
          "target",
          "amount",
          "timestamp"
        ],
        "title": "GraphEdge",
        "description": "Edge (transaction) in the graph."
      },
      "GraphNode": {
        "properties": {
          "id": {
            "type": "string",
            "title": "Id"
          },
          "label": {
            "type": "string",
            "title": "Label"
          },
          "group": {
            "type": "integer",
            "title": "Group",
            "description": "0=normal, 1=fraud, 2=suspicious",
            "default": 0
          },
          "amount_total": {
            "type": "number",
            "title": "Amount Total",
            "default": 0.0
          },
          "tx_count": {
            "type": "integer",
            "title": "Tx Count",
            "default": 0
          },
          "city": {
            "anyOf": [
              {
                "type": "string"
              },
              {
                "type": "null"
              }
            ],
            "title": "City"
          },
          "is_fraud": {
            "type": "boolean",
            "title": "Is Fraud",
            "default": false
          }
        },
        "type": "object",
        "required": [
          "id",
          "label"
        ],
        "title": "GraphNode",
        "description": "Node in the transaction graph."
      },
      "HTTPValidationError": {
        "properties": {
          "detail": {
            "items": {
              "$ref": "#/components/schemas/ValidationError"
            },
            "type": "array",
            "title": "Detail"
          }
        },
        "type": "object",
        "title": "HTTPValidationError"
      },
      "HealthResponse": {
        "properties": {
          "status": {
            "type": "string",
            "title": "Status",
            "description": "Overall system status",
            "default": "healthy"
          },
          "version": {
            "type": "string",
            "title": "Version",
            "default": "2.0.0"
          },
          "schema_version": {
            "type": "string",
            "title": "Schema Version",
            "default": "1.0.0"
          },
          "uptime_seconds": {
            "type": "number",
            "title": "Uptime Seconds",
            "default": 0.0
          },
          "components": {
            "additionalProperties": {
              "$ref": "#/components/schemas/ComponentStatus"
            },
            "type": "object",
            "title": "Components"
          }
        },
        "type": "object",
        "title": "HealthResponse",
        "description": "System health check response."
      },
      "KYCScreenRequest": {
        "properties": {
          "name": {
            "type": "string",
            "maxLength": 200,
            "minLength": 2,
            "title": "Name",
            "description": "Customer full name"
          },
          "country": {
            "anyOf": [
              {
                "type": "string"
              },
              {
                "type": "null"
              }
            ],
            "title": "Country",
            "description": "Country (ISO or name)"
          },
          "additional_info": {
            "anyOf": [
              {
                "additionalProperties": true,
                "type": "object"
              },
              {
                "type": "null"
              }
            ],
            "title": "Additional Info",
            "description": "Optional context (dob, id number, etc.)"
          }
        },
        "type": "object",
        "required": [
          "name"
        ],
        "title": "KYCScreenRequest",
        "description": "Request to screen a customer against PEP & sanctions lists."
      },
      "KYCScreenResponse": {
        "properties": {
          "screening_id": {
            "type": "string",
            "title": "Screening Id"
          },
          "query_name": {
            "type": "string",
            "title": "Query Name"
          },
          "screening_types": {
            "items": {
              "type": "string"
            },
            "type": "array",
            "title": "Screening Types"
          },
          "has_matches": {
            "type": "boolean",
            "title": "Has Matches"
          },
          "matches_count": {
            "type": "integer",
            "title": "Matches Count"
          },
          "matches": {
            "items": {
              "additionalProperties": true,
              "type": "object"
            },
            "type": "array",
            "title": "Matches"
          },
          "risk_score": {
            "type": "number",
            "maximum": 100.0,
            "minimum": 0.0,
            "title": "Risk Score",
            "default": 0.0
          },
          "recommendation": {
            "type": "string",
            "title": "Recommendation",
            "default": ""
          },
          "screened_at": {
            "type": "string",
            "title": "Screened At",
            "default": ""
          }
        },
        "type": "object",
        "required": [
          "screening_id",
          "query_name",
          "screening_types",
          "has_matches",
          "matches_count"
        ],
        "title": "KYCScreenResponse",
        "description": "Screening result combining PEP + sanctions checks."
      },
      "LoginRequest": {
        "properties": {
          "username": {
            "type": "string",
            "title": "Username",
            "description": "Username or email"
          },
          "password": {
            "type": "string",
            "title": "Password",
            "description": "Password"
          }
        },
        "type": "object",
        "required": [
          "username",
          "password"
        ],
        "title": "LoginRequest",
        "description": "Login request."
      },
      "ModelInfo": {
        "properties": {
          "name": {
            "type": "string",
            "title": "Name"
          },
          "version": {
            "type": "string",
            "title": "Version",
            "default": "1.0.0"
          },
          "ready": {
            "type": "boolean",
            "title": "Ready",
            "default": false
          },
          "last_trained": {
            "anyOf": [
              {
                "type": "string",
                "format": "date-time"
              },
              {
                "type": "null"
              }
            ],
            "title": "Last Trained"
          },
          "metrics": {
            "additionalProperties": {
              "type": "number"
            },
            "type": "object",
            "title": "Metrics"
          }
        },
        "type": "object",
        "required": [
          "name"
        ],
        "title": "ModelInfo",
        "description": "Information about a single ML model."
      },
      "ModelStatusResponse": {
        "properties": {
          "isolation_forest": {
            "$ref": "#/components/schemas/ModelInfo"
          },
          "xgboost": {
            "$ref": "#/components/schemas/ModelInfo"
          },
          "autoencoder": {
            "$ref": "#/components/schemas/ModelInfo"
          },
          "ensemble_ready": {
            "type": "boolean",
            "title": "Ensemble Ready",
            "default": false
          },
          "ensemble_threshold": {
            "type": "number",
            "title": "Ensemble Threshold",
            "default": 0.5
          }
        },
        "type": "object",
        "title": "ModelStatusResponse",
        "description": "Status of all ML models."
      },
      "PasswordChangeRequest": {
        "properties": {
          "current_password": {
            "type": "string",
            "title": "Current Password"
          },
          "new_password": {
            "type": "string",
            "minLength": 8,
            "title": "New Password"
          }
        },
        "type": "object",
        "required": [
          "current_password",
          "new_password"
        ],
        "title": "PasswordChangeRequest",
        "description": "Password change request."
      },
      "RefreshRequest": {
        "properties": {
          "refresh_token": {
            "type": "string",
            "title": "Refresh Token"
          }
        },
        "type": "object",
        "required": [
          "refresh_token"
        ],
        "title": "RefreshRequest",
        "description": "Token refresh request."
      },
      "RiskDecision": {
        "type": "string",
        "enum": [
          "allow",
          "review",
          "block",
          "critical"
        ],
        "title": "RiskDecision",
        "description": "Risk karar seviyeleri."
      },
      "RiskFactor": {
        "properties": {
          "feature": {
            "type": "string",
            "title": "Feature",
            "description": "Özellik adı"
          },
          "impact": {
            "type": "number",
            "title": "Impact",
            "description": "Risk etkisi (-1 to 1)"
          },
          "direction": {
            "type": "string",
            "title": "Direction",
            "description": "increases_risk / decreases_risk"
          },
          "explanation": {
            "type": "string",
            "title": "Explanation",
            "description": "Türkçe açıklama"
          },
          "value": {
            "anyOf": [
              {
                "type": "number"
              },
              {
                "type": "null"
              }
            ],
            "title": "Value",
            "description": "Özellik değeri"
          }
        },
        "type": "object",
        "required": [
          "feature",
          "impact",
          "direction",
          "explanation"
        ],
        "title": "RiskFactor",
        "description": "Tek bir risk faktörü açıklaması."
      },
      "RiskLevel": {
        "type": "string",
        "enum": [
          "low",
          "medium",
          "high",
          "critical"
        ],
        "title": "RiskLevel",
        "description": "Risk seviyeleri."
      },
      "RiskScoringRequest": {
        "properties": {
          "transaction_id": {
            "anyOf": [
              {
                "type": "string"
              },
              {
                "type": "null"
              }
            ],
            "title": "Transaction Id",
            "description": "İşlem ID"
          },
          "sender_iban": {
            "type": "string",
            "title": "Sender Iban",
            "description": "Gönderen IBAN"
          },
          "sender_name": {
            "type": "string",
            "title": "Sender Name",
            "description": "Gönderen adı"
          },
          "sender_city": {
            "type": "string",
            "title": "Sender City",
            "description": "Gönderen şehir",
            "default": "İstanbul"
          },
          "receiver_iban": {
            "type": "string",
            "title": "Receiver Iban",
            "description": "Alıcı IBAN"
          },
          "receiver_name": {
            "type": "string",
            "title": "Receiver Name",
            "description": "Alıcı adı"
          },
          "receiver_city": {
            "type": "string",
            "title": "Receiver City",
            "description": "Alıcı şehir",
            "default": "Ankara"
          },
          "amount": {
            "type": "number",
            "exclusiveMinimum": 0.0,
            "title": "Amount",
            "description": "Tutar (TL)"
          },
          "currency": {
            "type": "string",
            "title": "Currency",
            "description": "Para birimi",
            "default": "TRY"
          },
          "description": {
            "type": "string",
            "title": "Description",
            "description": "Açıklama",
            "default": ""
          },
          "timestamp": {
            "anyOf": [
              {
                "type": "string"
              },
              {
                "type": "null"
              }
            ],
            "title": "Timestamp",
            "description": "ISO 8601 timestamp"
          },
          "channel": {
            "type": "string",
            "title": "Channel",
            "description": "Kanal (mobile, web, atm)",
            "default": "mobile"
          },
          "device_id": {
            "anyOf": [
              {
                "type": "string"
              },
              {
                "type": "null"
              }
            ],
            "title": "Device Id",
            "description": "Cihaz ID"
          }
        },
        "type": "object",
        "required": [
          "sender_iban",
          "sender_name",
          "receiver_iban",
          "receiver_name",
          "amount"
        ],
        "title": "RiskScoringRequest",
        "description": "Risk skorlama isteği.",
        "example": {
          "amount": 15000.0,
          "channel": "mobile",
          "description": "Kira ödemesi",
          "receiver_city": "Ankara",
          "receiver_iban": "TR110006400000478893400002",
          "receiver_name": "Mehmet Kaya",
          "sender_city": "İstanbul",
          "sender_iban": "TR330006100519786457841326",
          "sender_name": "Ahmet Yılmaz"
        }
      },
      "RiskScoringResponse": {
        "properties": {
          "transaction_id": {
            "type": "string",
            "title": "Transaction Id"
          },
          "timestamp": {
            "type": "string",
            "title": "Timestamp"
          },
          "risk_score": {
            "type": "number",
            "maximum": 1.0,
            "minimum": 0.0,
            "title": "Risk Score",
            "description": "Risk skoru (0-1)"
          },
          "confidence": {
            "type": "number",
            "maximum": 1.0,
            "minimum": 0.0,
            "title": "Confidence",
            "description": "Güven skoru"
          },
          "decision": {
            "$ref": "#/components/schemas/RiskDecision"
          },
          "risk_level": {
            "$ref": "#/components/schemas/RiskLevel"
          },
          "latency_ms": {
            "type": "number",
            "title": "Latency Ms",
            "description": "İşlem süresi (ms)"
          },
          "model_scores": {
            "additionalProperties": {
              "type": "number"
            },
            "type": "object",
            "title": "Model Scores"
          },
          "ensemble_method": {
            "type": "string",
            "title": "Ensemble Method",
            "description": "Ensemble yöntemi",
            "default": "stacking"
          },
          "top_risk_factors": {
            "items": {
              "$ref": "#/components/schemas/RiskFactor"
            },
            "type": "array",
            "title": "Top Risk Factors"
          },
          "explanation_summary": {
            "type": "string",
            "title": "Explanation Summary",
            "description": "Özet açıklama",
            "default": ""
          },
          "similar_cases": {
            "items": {
              "$ref": "#/components/schemas/SimilarCase"
            },
            "type": "array",
            "title": "Similar Cases"
          },
          "recommended_action": {
            "type": "string",
            "title": "Recommended Action",
            "description": "Önerilen aksiyon",
            "default": ""
          },
          "num_features_extracted": {
            "type": "integer",
            "title": "Num Features Extracted",
            "default": 0
          }
        },
        "type": "object",
        "required": [
          "transaction_id",
          "timestamp",
          "risk_score",
          "confidence",
          "decision",
          "risk_level",
          "latency_ms"
        ],
        "title": "RiskScoringResponse",
        "description": "Risk skorlama yanıtı.",
        "example": {
          "confidence": 0.92,
          "decision": "block",
          "explanation_summary": "Yüksek tutarlı işlem, yeni alıcı, gece saatinde",
          "latency_ms": 25.4,
          "model_scores": {
            "CatBoost": 0.88,
            "LightGBM": 0.91,
            "XGBoost": 0.85
          },
          "risk_level": "high",
          "risk_score": 0.87,
          "timestamp": "2024-01-15T14:30:00Z",
          "transaction_id": "TX-ABC123"
        }
      },
      "Severity": {
        "type": "string",
        "enum": [
          "low",
          "medium",
          "high",
          "critical"
        ],
        "title": "Severity",
        "description": "Alert severity levels (aligned with SOC standards)."
      },
      "SimilarCase": {
        "properties": {
          "case_id": {
            "type": "string",
            "title": "Case Id"
          },
          "similarity_score": {
            "type": "number",
            "title": "Similarity Score"
          },
          "fraud_type": {
            "type": "string",
            "title": "Fraud Type"
          },
          "amount": {
            "type": "number",
            "title": "Amount"
          },
          "description": {
            "type": "string",
            "title": "Description"
          }
        },
        "type": "object",
        "required": [
          "case_id",
          "similarity_score",
          "fraud_type",
          "amount",
          "description"
        ],
        "title": "SimilarCase",
        "description": "Benzer fraud vakası."
      },
      "StatsResponse": {
        "properties": {
          "transactions_processed": {
            "type": "integer",
            "title": "Transactions Processed",
            "default": 0
          },
          "fraud_detected": {
            "type": "integer",
            "title": "Fraud Detected",
            "default": 0
          },
          "alerts_created": {
            "type": "integer",
            "title": "Alerts Created",
            "default": 0
          },
          "cases_open": {
            "type": "integer",
            "title": "Cases Open",
            "default": 0
          },
          "cases_resolved": {
            "type": "integer",
            "title": "Cases Resolved",
            "default": 0
          },
          "by_fraud_type": {
            "additionalProperties": {
              "type": "integer"
            },
            "type": "object",
            "title": "By Fraud Type"
          },
          "by_severity": {
            "additionalProperties": {
              "type": "integer"
            },
            "type": "object",
            "title": "By Severity"
          },
          "avg_detection_latency_ms": {
            "type": "number",
            "title": "Avg Detection Latency Ms",
            "default": 0.0
          },
          "p95_detection_latency_ms": {
            "type": "number",
            "title": "P95 Detection Latency Ms",
            "default": 0.0
          },
          "period_start": {
            "anyOf": [
              {
                "type": "string",
                "format": "date-time"
              },
              {
                "type": "null"
              }
            ],
            "title": "Period Start"
          },
          "period_end": {
            "anyOf": [
              {
                "type": "string",
                "format": "date-time"
              },
              {
                "type": "null"
              }
            ],
            "title": "Period End"
          },
          "uptime_seconds": {
            "type": "number",
            "title": "Uptime Seconds",
            "default": 0.0
          },
          "fraud_rate": {
            "type": "number",
            "title": "Fraud Rate",
            "description": "Fraud detection rate (percentage)",
            "default": 0.0
          },
          "alerts_per_minute": {
            "type": "number",
            "title": "Alerts Per Minute",
            "default": 0.0
          }
        },
        "type": "object",
        "title": "StatsResponse",
        "description": "System statistics response."
      },
      "TokenResponse": {
        "properties": {
          "access_token": {
            "type": "string",
            "title": "Access Token"
          },
          "refresh_token": {
            "anyOf": [
              {
                "type": "string"
              },
              {
                "type": "null"
              }
            ],
            "title": "Refresh Token"
          },
          "token_type": {
            "type": "string",
            "title": "Token Type",
            "default": "bearer"
          },
          "expires_in": {
            "type": "integer",
            "title": "Expires In",
            "description": "Token expiry in seconds",
            "default": 3600
          }
        },
        "type": "object",
        "required": [
          "access_token"
        ],
        "title": "TokenResponse",
        "description": "JWT token response."
      },
      "TrainRequest": {
        "properties": {
          "n_samples": {
            "type": "integer",
            "maximum": 100000.0,
            "minimum": 100.0,
            "title": "N Samples",
            "description": "Number of synthetic samples",
            "default": 5000
          },
          "fraud_ratio": {
            "type": "number",
            "maximum": 0.5,
            "minimum": 0.01,
            "title": "Fraud Ratio",
            "description": "Fraud ratio",
            "default": 0.05
          }
        },
        "type": "object",
        "title": "TrainRequest",
        "description": "Request to trigger model training."
      },
      "TrainResponse": {
        "properties": {
          "status": {
            "type": "string",
            "title": "Status",
            "default": "completed"
          },
          "training_time_seconds": {
            "type": "number",
            "title": "Training Time Seconds",
            "default": 0.0
          },
          "dataset_size": {
            "type": "integer",
            "title": "Dataset Size",
            "default": 0
          },
          "metrics": {
            "additionalProperties": true,
            "type": "object",
            "title": "Metrics"
          }
        },
        "type": "object",
        "title": "TrainResponse",
        "description": "Response with training results."
      },
      "TransactionCreate": {
        "properties": {
          "transaction_id": {
            "anyOf": [
              {
                "type": "string"
              },
              {
                "type": "null"
              }
            ],
            "title": "Transaction Id",
            "description": "Unique transaction ID (auto-generated if empty)",
            "examples": [
              "TXN-A1B2C3D4E5F6"
            ]
          },
          "sender_iban": {
            "type": "string",
            "maxLength": 34,
            "minLength": 15,
            "title": "Sender Iban",
            "description": "Sender IBAN",
            "examples": [
              "TR330006100519786457841326"
            ]
          },
          "sender_name": {
            "type": "string",
            "maxLength": 200,
            "minLength": 1,
            "title": "Sender Name",
            "description": "Sender full name",
            "examples": [
              "Ahmet Yılmaz"
            ]
          },
          "sender_city": {
            "type": "string",
            "maxLength": 100,
            "title": "Sender City",
            "description": "Sender city",
            "default": "",
            "examples": [
              "İstanbul"
            ]
          },
          "receiver_iban": {
            "type": "string",
            "maxLength": 34,
            "minLength": 15,
            "title": "Receiver Iban",
            "description": "Receiver IBAN",
            "examples": [
              "TR110006400000468521793064"
            ]
          },
          "receiver_name": {
            "type": "string",
            "maxLength": 200,
            "minLength": 1,
            "title": "Receiver Name",
            "description": "Receiver full name",
            "examples": [
              "Mehmet Demir"
            ]
          },
          "receiver_city": {
            "type": "string",
            "maxLength": 100,
            "title": "Receiver City",
            "description": "Receiver city",
            "default": "",
            "examples": [
              "Ankara"
            ]
          },
          "amount": {
            "type": "number",
            "exclusiveMinimum": 0.0,
            "title": "Amount",
            "description": "Transfer amount",
            "examples": [
              25000.0
            ]
          },
          "currency": {
            "type": "string",
            "maxLength": 3,
            "title": "Currency",
            "description": "Currency code (ISO 4217)",
            "default": "TRY"
          },
          "description": {
            "type": "string",
            "maxLength": 500,
            "title": "Description",
            "description": "Transaction description/note",
            "default": "",
            "examples": [
              "Kira ödemesi"
            ]
          },
          "timestamp": {
            "anyOf": [
              {
                "type": "string",
                "format": "date-time"
              },
              {
                "type": "null"
              }
            ],
            "title": "Timestamp",
            "description": "Transaction timestamp (auto-set if empty)"
          },
          "sender_latitude": {
            "anyOf": [
              {
                "type": "number",
                "maximum": 90.0,
                "minimum": -90.0
              },
              {
                "type": "null"
              }
            ],
            "title": "Sender Latitude"
          },
          "sender_longitude": {
            "anyOf": [
              {
                "type": "number",
                "maximum": 180.0,
                "minimum": -180.0
              },
              {
                "type": "null"
              }
            ],
            "title": "Sender Longitude"
          },
          "sender_ip": {
            "anyOf": [
              {
                "type": "string",
                "maxLength": 45
              },
              {
                "type": "null"
              }
            ],
            "title": "Sender Ip"
          },
          "device_id": {
            "anyOf": [
              {
                "type": "string",
                "maxLength": 100
              },
              {
                "type": "null"
              }
            ],
            "title": "Device Id"
          },
          "channel": {
            "type": "string",
            "title": "Channel",
            "description": "Transaction channel: web, mobile, atm, branch",
            "default": "web"
          }
        },
        "type": "object",
        "required": [
          "sender_iban",
          "sender_name",
          "receiver_iban",
          "receiver_name",
          "amount"
        ],
        "title": "TransactionCreate",
        "description": "Schema for creating/submitting a transaction.\nUsed by API endpoints and Kafka producers."
      },
      "TransactionResponse": {
        "properties": {
          "transaction_id": {
            "type": "string",
            "title": "Transaction Id"
          },
          "status": {
            "type": "string",
            "title": "Status",
            "description": "Processing status",
            "default": "analyzed"
          },
          "message": {
            "type": "string",
            "title": "Message",
            "default": "Transaction analyzed successfully"
          },
          "is_fraud": {
            "type": "boolean",
            "title": "Is Fraud",
            "description": "Fraud detection result",
            "default": false
          },
          "fraud_score": {
            "type": "number",
            "maximum": 1.0,
            "minimum": 0.0,
            "title": "Fraud Score",
            "description": "ML ensemble score",
            "default": 0.0
          },
          "alerts": {
            "anyOf": [
              {
                "items": {
                  "$ref": "#/components/schemas/Alert"
                },
                "type": "array"
              },
              {
                "type": "null"
              }
            ],
            "title": "Alerts",
            "description": "Generated alerts, if any"
          },
          "processing_time_ms": {
            "type": "number",
            "title": "Processing Time Ms",
            "default": 0.0
          }
        },
        "type": "object",
        "required": [
          "transaction_id"
        ],
        "title": "TransactionResponse",
        "description": "Response after submitting a transaction."
      },
      "User": {
        "properties": {
          "user_id": {
            "type": "string",
            "title": "User Id"
          },
          "username": {
            "type": "string",
            "title": "Username"
          },
          "email": {
            "type": "string",
            "title": "Email"
          },
          "full_name": {
            "type": "string",
            "title": "Full Name"
          },
          "role": {
            "$ref": "#/components/schemas/UserRole",
            "default": "viewer"
          },
          "status": {
            "$ref": "#/components/schemas/UserStatus",
            "default": "active"
          },
          "team": {
            "anyOf": [
              {
                "type": "string"
              },
              {
                "type": "null"
              }
            ],
            "title": "Team"
          },
          "created_at": {
            "type": "string",
            "format": "date-time",
            "title": "Created At"
          },
          "updated_at": {
            "type": "string",
            "format": "date-time",
            "title": "Updated At"
          },
          "last_login": {
            "anyOf": [
              {
                "type": "string",
                "format": "date-time"
              },
              {
                "type": "null"
              }
            ],
            "title": "Last Login"
          },
          "preferences": {
            "additionalProperties": true,
            "type": "object",
            "title": "Preferences"
          }
        },
        "type": "object",
        "required": [
          "username",
          "email",
          "full_name"
        ],
        "title": "User",
        "description": "Full user record."
      },
      "UserCreate": {
        "properties": {
          "username": {
            "type": "string",
            "maxLength": 50,
            "minLength": 3,
            "pattern": "^[a-zA-Z0-9_]+$",
            "title": "Username",
            "description": "Username (alphanumeric + underscore)"
          },
          "email": {
            "type": "string",
            "format": "email",
            "title": "Email",
            "description": "Email address"
          },
          "password": {
            "type": "string",
            "maxLength": 100,
            "minLength": 8,
            "title": "Password",
            "description": "Password (min 8 characters)"
          },
          "full_name": {
            "type": "string",
            "maxLength": 200,
            "minLength": 1,
            "title": "Full Name",
            "description": "Full name"
          },
          "role": {
            "$ref": "#/components/schemas/UserRole",
            "default": "viewer"
          },
          "team": {
            "anyOf": [
              {
                "type": "string",
                "maxLength": 100
              },
              {
                "type": "null"
              }
            ],
            "title": "Team"
          }
        },
        "type": "object",
        "required": [
          "username",
          "email",
          "password",
          "full_name"
        ],
        "title": "UserCreate",
        "description": "Schema for creating a new user."
      },
      "UserPublic": {
        "properties": {
          "user_id": {
            "type": "string",
            "title": "User Id"
          },
          "username": {
            "type": "string",
            "title": "Username"
          },
          "full_name": {
            "type": "string",
            "title": "Full Name"
          },
          "role": {
            "$ref": "#/components/schemas/UserRole"
          },
          "team": {
            "anyOf": [
              {
                "type": "string"
              },
              {
                "type": "null"
              }
            ],
            "title": "Team"
          }
        },
        "type": "object",
        "required": [
          "user_id",
          "username",
          "full_name",
          "role"
        ],
        "title": "UserPublic",
        "description": "Public user info (no sensitive data)."
      },
      "UserRole": {
        "type": "string",
        "enum": [
          "admin",
          "analyst",
          "viewer",
          "api"
        ],
        "title": "UserRole",
        "description": "User roles for RBAC."
      },
      "UserStatus": {
        "type": "string",
        "enum": [
          "active",
          "inactive",
          "suspended",
          "pending"
        ],
        "title": "UserStatus",
        "description": "User account status."
      },
      "ValidationError": {
        "properties": {
          "loc": {
            "items": {
              "anyOf": [
                {
                  "type": "string"
                },
                {
                  "type": "integer"
                }
              ]
            },
            "type": "array",
            "title": "Location"
          },
          "msg": {
            "type": "string",
            "title": "Message"
          },
          "type": {
            "type": "string",
            "title": "Error Type"
          },
          "input": {
            "title": "Input"
          },
          "ctx": {
            "type": "object",
            "title": "Context"
          }
        },
        "type": "object",
        "required": [
          "loc",
          "msg",
          "type"
        ],
        "title": "ValidationError"
      }
    },
    "securitySchemes": {
      "HTTPBearer": {
        "type": "http",
        "scheme": "bearer"
      }
    }
  }
} as const;

export type ApiSchema = typeof apiSchema;
