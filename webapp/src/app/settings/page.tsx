"use client";

import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Settings } from "lucide-react";

import { SettingsManager } from "@/components/dashboard/SettingsManager";

export default function SettingsPage() {
  return (
    <div className="space-y-6">
      <div className="px-6 pt-6">
        <h1 className="text-3xl font-bold tracking-tight mb-2 uppercase">Infrastructure Settings</h1>
        <p className="text-muted-foreground font-mono text-xs uppercase">Global registry management and parameter tuning.</p>
      </div>

      <SettingsManager />
    </div>
  );
}
