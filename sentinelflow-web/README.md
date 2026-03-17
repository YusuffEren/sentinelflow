# SentinelFlow Web Dashboard

Modern Real-Time Fraud Detection Dashboard built with Next.js, Tailwind CSS, and WebSockets.

## Features

- 🛡️ **Cyber/SOC Aesthetic**: Designed for security analysts.
- ⚡ **Real-Time**: Live updates via WebSockets.
- 📊 **Visualizations**: Fraud rings, transaction volume, and severity feeds.

## Getting Started

1.  **Install dependencies**:
    ```bash
    npm install
    ```

2.  **Run the development server**:
    ```bash
    npm run dev
    ```

3.  **Open [http://localhost:3000](http://localhost:3000)** with your browser.

## Backend Connection

This frontend connects to the SentinelFlow API at `ws://localhost:8000/ws/alerts`.
Ensure the backend is running before starting the frontend.
