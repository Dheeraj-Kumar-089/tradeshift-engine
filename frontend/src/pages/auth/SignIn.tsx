import React from 'react';
import { useForm } from 'react-hook-form';
import { zodResolver } from '@hookform/resolvers/zod';
import * as z from 'zod';
import { Link, useNavigate } from 'react-router-dom';
import { toast } from 'sonner';
import { useAuth } from '@/context/AuthContext';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import { Label } from '@/components/ui/label';
import { Card, CardContent, CardDescription, CardFooter, CardHeader, CardTitle } from '@/components/ui/card';

const signInSchema = z.object({
    email: z.string().email('Invalid email address'),
    password: z.string().min(1, 'Password is required'),
});

type SignInValues = z.infer<typeof signInSchema>;

const SignIn = () => {
    const { login } = useAuth();
    const navigate = useNavigate();
    const [error, setError] = React.useState<string | null>(null);

    const {
        register,
        handleSubmit,
        formState: { errors, isSubmitting },
    } = useForm<SignInValues>({
        resolver: zodResolver(signInSchema),
    });

    const onSubmit = async (data: SignInValues) => {
        try {
            setError(null);
            await login(data);
            toast.success("Signed in successfully!");
            navigate('/'); // Redirect to dashboard/home after login
        } catch (err: any) {
            console.error("Login failed", err);
            const errorMessage = err.response?.data?.detail || "Invalid email or password";
            setError(errorMessage);
            toast.error("Login failed", {
                description: errorMessage
            });
        }
    };

    return (
        <div className="flex h-full w-full items-center justify-center">
            <Card className="w-full max-w-md border border-white/10 bg-surface/40 backdrop-blur-xl shadow-2xl relative overflow-hidden group">
                {/* Subtle sheen effect */}
                <div className="absolute inset-0 bg-gradient-to-br from-white/5 to-transparent pointer-events-none" />

                <CardHeader className="space-y-1 text-center pb-8 border-b border-white/5">
                    <CardTitle className="text-2xl font-bold tracking-tight text-white">Welcome Back</CardTitle>
                    <CardDescription className="text-slate-400">
                        Enter your credentials to access the terminal
                    </CardDescription>
                </CardHeader>
                <CardContent className="pt-8 space-y-6">
                    <form onSubmit={handleSubmit(onSubmit)} className="space-y-5">
                        {error && (
                            <div className="p-3 rounded-md bg-rose-500/10 text-rose-400 text-sm font-medium border border-rose-500/20 flex items-center gap-2">
                                <span className="h-1.5 w-1.5 rounded-full bg-rose-500" />
                                {error}
                            </div>
                        )}
                        <div className="space-y-2">
                            <Label htmlFor="email" className="text-slate-300 font-medium">Email</Label>
                            <Input
                                id="email"
                                placeholder="name@example.com"
                                className="bg-slate-950/50 border-white/10 text-white placeholder:text-slate-600 focus:border-blue-500/50 focus:ring-blue-500/20 h-11 transition-all"
                                {...register('email')}
                            />
                            {errors.email && <p className="text-rose-400 text-xs mt-1">{errors.email.message}</p>}
                        </div>
                        <div className="space-y-2">
                            <div className="flex items-center justify-between">
                                <Label htmlFor="password" className="text-slate-300 font-medium">Password</Label>
                                <Link
                                    to="/auth/forgot-password"
                                    className="text-xs text-blue-400 hover:text-blue-300 transition-colors"
                                >
                                    Forgot password?
                                </Link>
                            </div>
                            <Input
                                id="password"
                                type="password"
                                className="bg-slate-950/50 border-white/10 text-white focus:border-blue-500/50 focus:ring-blue-500/20 h-11 transition-all"
                                {...register('password')}
                            />
                            {errors.password && <p className="text-rose-400 text-xs mt-1">{errors.password.message}</p>}
                        </div>
                        <Button
                            className="w-full h-11 bg-gradient-to-r from-blue-600 to-blue-700 hover:from-blue-500 hover:to-blue-600 text-white shadow-lg shadow-blue-600/20 transition-all hover:scale-[1.02] active:scale-[0.98] font-medium tracking-wide"
                            type="submit"
                            disabled={isSubmitting}
                        >
                            {isSubmitting ? (
                                <div className="flex items-center gap-2">
                                    <div className="h-4 w-4 animate-spin rounded-full border-2 border-white/20 border-t-white" />
                                    <span>Signing in...</span>
                                </div>
                            ) : 'Sign In'}
                        </Button>
                    </form>
                </CardContent>
                <CardFooter className="flex justify-center border-t border-white/5 bg-surface/30 p-6">
                    <p className="text-sm text-slate-400">
                        Don't have an account?{' '}
                        <Link to="/auth/sign-up" className="font-medium text-blue-400 hover:text-blue-300 hover:underline transition-colors">
                            Create account
                        </Link>
                    </p>
                </CardFooter>
            </Card>
        </div>
    );
};

export default SignIn;
