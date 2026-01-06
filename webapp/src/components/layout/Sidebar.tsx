"use client";

import Link from "next/link";
import { useEffect, useState } from "react";
import { usePathname } from "next/navigation";
import { cn } from "@/lib/utils";
import { 
  LayoutDashboard, 
  FlaskConical, 
  Settings, 
  ShieldAlert, 
  Activity,
  ChevronRight
} from "lucide-react";
import { api } from "@/lib/api";

interface SidebarProps extends React.HTMLAttributes<HTMLDivElement> {}

const navItems = [
  { name: "Overview", href: "/", icon: LayoutDashboard },
  { name: "Infrastructure", href: "/infra", icon: ShieldAlert },
  { name: "Settings", href: "/settings", icon: Settings },
];

export function Sidebar({ className }: SidebarProps) {
  const pathname = usePathname();
  const [isOnline, setIsOnline] = useState(false);

  useEffect(() => {
    const checkHealth = async () => {
      try {
        await api.getHealth();
        setIsOnline(true);
      } catch (err) {
        setIsOnline(false);
      }
    };
    checkHealth();
    const interval = setInterval(checkHealth, 10000);
    return () => clearInterval(interval);
  }, []);

  return (
    <div className={cn("flex flex-col h-full bg-muted/30", className)}>
      <div className="p-6">
        <div className="flex items-center gap-2 mb-8 px-2">
          <div className="size-6 bg-primary rounded-sm flex items-center justify-center">
            <Activity className="size-4 text-primary-foreground" />
          </div>
          <span className="font-semibold text-lg tracking-tight text-white">Workbench</span>
        </div>

        <nav className="space-y-1">
          {navItems.map((item) => (
            <Link
              key={item.href}
              href={item.href}
              className={cn(
                "flex items-center gap-3 px-3 py-2 rounded-md text-sm font-medium transition-colors",
                pathname === item.href 
                  ? "bg-primary/10 text-primary" 
                  : "text-muted-foreground hover:bg-accent hover:text-accent-foreground"
              )}
            >
              <item.icon className="size-4" />
              {item.name}
              {pathname === item.href && (
                <ChevronRight className="ml-auto size-3" />
              )}
            </Link>
          ))}
        </nav>
      </div>

      <div className="mt-auto p-6 border-t font-mono text-[10px] text-muted-foreground">
        <div className="flex items-center gap-2 mb-1 uppercase tracking-wider">
          <div className={cn("size-1.5 rounded-full", isOnline ? "bg-green-500 animate-pulse" : "bg-red-500")} />
          {isOnline ? "System Online" : "System Offline"}
        </div>
        <div className="text-muted-foreground/50 italic">Internal Utility</div>
      </div>
    </div>
  );
}
