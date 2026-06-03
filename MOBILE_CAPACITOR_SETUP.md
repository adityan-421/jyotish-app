# Mobile (Capacitor wrapper) — setup & release runbook

The mobile app is a Capacitor native shell that loads the live web portal
(`https://grahalogic.ai`). Because it's the real web app, it's always at parity —
no per-feature porting. The only native-specific piece is **auth** (Google blocks
OAuth inside embedded webviews), handled by signing in natively and calling the
API with a **Bearer token** (the backend already supports this).

## What's already done (in this repo)
- `templates/index.html` has a **Capacitor-mode** `<script>` (in `<head>`): it activates
  only inside the native shell, attaches `Authorization: Bearer <token>` to `/api` and
  `/auth` calls, and routes the "Sign in with Google" buttons to native sign-in →
  `POST /auth/mobile` → stores the token in `localStorage`. No-op in a normal browser.
- Safe-area CSS under `html.capacitor` (header clears the notch, body clears the home bar).
- `capacitor.config.json` → `plugins.GoogleAuth` configured with the iOS + web client IDs.
- `package.json` adds `@codetrix-studio/capacitor-google-auth`, `@capacitor/status-bar`,
  `@capacitor/app`. `npm install` has been run; `npx cap copy ios` has synced the config.

## Already done & verified on this machine
- `npx cap sync ios` ran clean (Xcode 26.2): plugins `@codetrix-studio/capacitor-google-auth`,
  `@capacitor/status-bar`, `@capacitor/app` linked; pods installed.
- The **Google sign-in URL scheme** (reversed iOS client ID
  `com.googleusercontent.apps.333157384151-tbu6skhnta08056eaktdtfpcd0cgkium`) is already added
  to `ios/App/App/Info.plist`.
- A simulator build (`xcodebuild ... -sdk iphonesimulator`) returned **BUILD SUCCEEDED**.

## Prerequisites for release
- Apple Developer account (the iOS OAuth client `333157384151-tbu6...` already exists).
- Confirm that iOS OAuth client's bundle id is `ai.grahalogic.app` in Google Cloud Console.

## Steps (remaining — all in Xcode)
1. **Open & run:**
   ```bash
   cd jyotish_app
   npx cap open ios        # opens ios/App/App.xcworkspace in Xcode
   ```
   Set the Signing **Team** (App target → Signing & Capabilities), pick a simulator/device, Run.
4. **Test sign-in:** tap "Sign in with Google" → native Google sheet → it should return to the
   app **logged in** (header shows your name). Watch the network log: `POST /auth/mobile`
   returns `{token, user}`, then `/api/me` succeeds with the `Authorization: Bearer` header.
5. **Exercise the app:** generate a chart, **save it**, open My Charts, generate an AI reading,
   toggle **Layman/Expert**, run a **compatibility** reading, open **BTR** — all work because
   it's the live portal authenticated by the Bearer token.
6. **Sign out** clears the token; protected calls then 401 and the UI shows signed-out.

## ⚠️ Verify on first device test — the auth handshake
The web script prefers the plugin's **Google ID token** and posts it as `id_token` to
`/auth/mobile`, which verifies it via Google's `tokeninfo` (no client secret, no code
exchange — the simplest path; see `app.py:495`). If the plugin returns no `idToken` and only
a `serverAuthCode`, it falls back to `auth_code`, which `/auth/mobile` exchanges using
`redirect_uri` default `grahalogic://` (`app.py:463`) — that redirect URI may need to match
your plugin/OAuth config. **Expect to confirm/tweak this one handshake the first time you can
run on device.** Easiest fix if needed: ensure the plugin returns an `idToken` (don't rely on
`serverAuthCode`).

## Phase 2 — push notifications (optional)
- Add `@capacitor/push-notifications`; register the device token → `POST /api/push-token`
  (`app.py:1583`, already exists).
- **Backend gap:** `/api/cron/daily-notifications` (`app.py:1643`) generates the daily content
  but does **not** deliver via APNs/FCM yet. Add an APNs/FCM send step to actually push.

## Release
- App icon + splash (replace default Capacitor assets), App Store metadata, screenshots.
- Archive in Xcode → TestFlight → App Store. (Privacy policy already lives at `/privacy`.)
- Android later: `npx cap add android` (the web script + auth flow are platform-agnostic).
