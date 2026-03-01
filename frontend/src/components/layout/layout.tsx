import { Outlet } from 'react-router-dom';
import Topbar from './Header';

const Layout = () => {
    return (
        <main className="h-screen w-screen flex flex-col overflow-hidden bg-tv-bg-base text-tv-text-primary">
            <Topbar />
            <div className="flex-1 flex overflow-hidden">
                <Outlet />
            </div>
        </main>
    );
};
export default Layout;
