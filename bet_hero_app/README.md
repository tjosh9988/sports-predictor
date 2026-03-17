# AccaAI - AI-Powered Accumulator Predictions

AccaAI is a multi-sport accumulator prediction platform powered by machine learning. It provides data-driven insights for Football, Basketball, Tennis, NFL, Cricket, NHL, and MLB.

## Features
- **AI Accumulators**: Daily 3-odds, 5-odds, and 10-odds combos.
- **Match Center**: Deep statistics, H2H history, and team form.
- **Push Notifications**: Real-time alerts for new predictions and match results.
- **ROI Tracking**: Comprehensive statistics on model performance and accuracy.

## Prerequisites
- Flutter SDK (Latest Stable)
- Dart SDK
- Android Studio / Xcode (for builds)

## Setup Instructions

1. **Clone and Install**
   ```bash
   git clone <repo-url>
   cd bet_hero_app
   flutter pub get
   ```

2. **Environment Variables**
   - Copy `.env.example` to `.env`
   - Fill in your `BACKEND_URL`, `SUPABASE_URL`, and `SUPABASE_ANON_KEY`.

3. **Firebase Setup**
   - Place `google-services.json` in `android/app/`
   - Place `GoogleService-Info.plist` in `ios/Runner/`

4. **Run Application**
   ```bash
   make run
   ```

## Build Instructions

### Android Build (Release APK)
```bash
make build-android
```

### iOS Build (Release IPA)
```bash
make build-ios
```

## Project Structure
- `lib/core`: Routing, theme, and configuration.
- `lib/data`: Models and repository implementations.
- `lib/presentation`: UI screens, providers, and reusable widgets.
- `lib/services`: Notification and Authentication logic.

## Backend Connection
The app connects to a FastAPI backend deployed on Render. Ensure the `BACKEND_URL` in your `.env` is correctly pointing to your production or staging API.
