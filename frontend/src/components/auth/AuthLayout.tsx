import React from 'react';
import { Outlet } from 'react-router-dom';
import Header from '../layout/Header';

const AuthLayout = () => {
    return (
        <div className="flex min-h-screen w-full flex-col bg-background font-geist selection:bg-primary/20">
            <Header />
            <div className="flex flex-1">
                {/* Left Side - Form */}
                <div className="flex w-full flex-col justify-center px-4 py-12 sm:px-6 lg:flex-none lg:w-1/2 xl:px-24 bg-background relative overflow-hidden">
                    {/* Background Gradients for depth */}
                    <div className="absolute top-[-20%] left-[-10%] w-[50%] h-[50%] rounded-full bg-blue-600/5 blur-[120px]" />
                    <div className="absolute bottom-[-20%] right-[-10%] w-[50%] h-[50%] rounded-full bg-emerald-500/5 blur-[120px]" />

                    <div className="mx-auto w-full max-w-sm lg:w-96 relative z-10">
                        <Outlet />
                    </div>
                </div>

                {/* Right Side - Visual */}
                <div className="relative hidden w-0 flex-1 lg:block bg-surface border-l border-border overflow-hidden">
                    <div className="absolute inset-0 bg-gradient-to-br from-blue-900/40 via-surface to-surface" />
                    <div className="absolute inset-0 bg-[url('/grid-pattern.svg')] opacity-[0.03]" />

                    {/* Abstract Shapes */}
                    <div className="absolute top-1/2 left-1/2 -translate-x-1/2 -translate-y-1/2 w-[600px] h-[600px] bg-blue-500/10 rounded-full blur-[100px] animate-pulse" />

                    <div className="relative z-10 flex h-full flex-col justify-center items-center p-10 text-foreground">
                        <div className="max-w-md text-center backdrop-blur-sm bg-surface/30 p-8 rounded-3xl border border-white/5 shadow-2xl">
                            <div className="mb-8 flex justify-center">
                                <div className="h-20 w-20 rounded-2xl bg-gradient-to-br from-blue-500 to-blue-600 flex items-center justify-center shadow-lg shadow-blue-500/30 ring-4 ring-white/5">
                                    <svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" className="h-10 w-10 text-white"><path d="M3 3v18h18" /><path d="m19 9-5 5-4-4-3 3" /></svg>
                                </div>
                            </div>
                            <h2 className="text-3xl font-bold tracking-tight text-white mb-4">
                                Master the Markets
                            </h2>
                            <p className="text-lg text-slate-400 leading-relaxed">
                                "TradeSim gives you the professional edge. Real-time simulation, advanced analytics, and a risk-free environment to perfect your strategy."
                            </p>

                            <div className="mt-8 flex items-center justify-center gap-2 text-sm font-medium text-slate-500">
                                <div className="flex -space-x-2">
                                    {[1, 2, 3].map((i) => (
                                        <div key={i} className="h-8 w-8 rounded-full border-2 border-surface bg-slate-700" />
                                    ))}
                                </div>
                                <span>Joined by 10,000+ Traders</span>
                            </div>
                        </div>
                    </div>
                </div>
            </div>
        </div>
    );
};

export default AuthLayout;
