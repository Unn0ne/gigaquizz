# Vendored QR encoder

- Project: Project Nayuki, QR Code generator library.
- License: MIT; full upstream notice is retained in qrcodegen.js and qrcodegen.LICENSE.txt.
- Version: v1.8.0, official precompiled ES6 JavaScript release asset.
- Upstream tag object: 7ad95cedd8464a87f82221283612732ae4f3f305.
- Upstream source commit: 720f62bddb7226106071d4728c292cb1df519ceb.
- Release: https://github.com/nayuki/QR-Code-generator/releases/tag/v1.8.0
- Download: https://github.com/nayuki/QR-Code-generator/releases/download/v1.8.0/qrcodegen-v1.8.0-es6.js
- TypeScript source: https://github.com/nayuki/QR-Code-generator/blob/720f62bddb7226106071d4728c292cb1df519ceb/typescript-javascript/qrcodegen.ts
- Project documentation: https://www.nayuki.io/page/qr-code-generator-library
- Retrieved: 2026-09-12.
- Asset size: 45336 bytes.
- SHA-256: `6a1116192ed1dd67fa1bf31e77f5817103d71c23bbac24c382e698b7668bdd01`.

The downloaded JavaScript is byte-for-byte unchanged; no minification or custom
encoder changes. The upstream release supplies compiled JavaScript separately
from its TypeScript source tree. No build step or package download is required
for the application: admin.html loads the vendored same-origin asset.

The application renders a white, opaque canvas with a four-module quiet zone,
black modules at an integer eight-pixel scale, and MEDIUM error correction
(with upstream automatic strengthening when it fits the same QR version).
The URL is the exact value of the selected poll's share-link field. Canvas PNG
export contains only the QR; it does not add identifiers, logos or tracking URLs.
