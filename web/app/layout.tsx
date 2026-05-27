import type { Metadata } from "next";
import "./globals.css";

export const metadata: Metadata = {
  title: "Rock Evolution Dashboard",
  description: "An interactive analysis of rock's Billboard Hot 100 rise, peak, fragmentation, and decline."
};

export default function RootLayout({ children }: { children: React.ReactNode }) {
  return (
    <html lang="en">
      <body>{children}</body>
    </html>
  );
}
