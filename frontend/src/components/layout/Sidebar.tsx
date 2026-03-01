import { LayoutDashboard, History, Settings, TrendingUp } from 'lucide-react';

export type Page = 'terminal' | 'history' | 'settings';

interface SidebarProps {
  page: Page;
  setPage: (p: Page) => void;
}

const Sidebar = ({ page, setPage }: SidebarProps) => {

  const NavItem = ({ p, icon: Icon, label }: { p: Page, icon: any, label: string; }) => (
    <button
      onClick={() => setPage(p)}
      className={`w-full p-4 flex flex-col items-center gap-2 transition-all duration-300 group relative
        ${page === p
          ? 'text-sidebar-primary'
          : 'text-sidebar-foreground/60 hover:text-sidebar-primary'
        }
      `}
    >
      {/* Active Indicator Line (Animated) */}
      <div className={`absolute left-0 top-1/2 -translate-y-1/2 w-1 h-8 rounded-r-full bg-sidebar-primary transition-all duration-300 
        ${page === p ? 'opacity-100 scale-100' : 'opacity-0 scale-0'}
      `} />

      {/* Icon with Hover Animation */}
      <div className={`transition-transform duration-300 group-hover:scale-110 group-active:scale-95`}>
        <Icon size={24} />
      </div>

      {/* Label */}
      <span className="text-[10px] font-bold tracking-wide opacity-80">{label}</span>
    </button>
  );

  return (
    <div className="w-20 border-r border-sidebar-border bg-sidebar flex flex-col h-full z-20 transition-colors duration-300">

      {/* Logo Area */}
      <div className="h-16 flex items-center justify-center border-b border-sidebar-border mb-4">
        <div className="bg-sidebar-primary/10 p-2 rounded-xl transition-transform hover:rotate-12 border border-sidebar-primary/20">
          <TrendingUp className="text-sidebar-primary" size={24} />
        </div>
      </div>

      <NavItem p="terminal" icon={LayoutDashboard} label="Trade" />
      <NavItem p="history" icon={History} label="History" />
      <NavItem p="settings" icon={Settings} label="Config" />
    </div>
  );
};

export default Sidebar;