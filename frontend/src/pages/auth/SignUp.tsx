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
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";

const signUpSchema = z.object({
    full_name: z.string().min(2, 'Name must be at least 2 characters').optional(),
    email: z.string().email('Invalid email address').refine((val) => val.endsWith('@gmail.com'), {
        message: "Only @gmail.com addresses are allowed"
    }),
    password: z.string().min(6, 'Password must be at least 6 characters'),
    confirmPassword: z.string(),
    country: z.string().min(1, 'Country is required'),
    investment_goals: z.string().min(1, 'Investment goal is required'),
    risk_tolerance: z.string().min(1, 'Risk tolerance is required'),
    preferred_industries: z.string().min(1, 'Preferred industry is required'),
}).refine((data) => data.password === data.confirmPassword, {
    message: "Passwords don't match",
    path: ["confirmPassword"],
});

type SignUpValues = z.infer<typeof signUpSchema>;

const SignUp = () => {
    const { register: registerUser } = useAuth();
    const navigate = useNavigate();
    const [error, setError] = React.useState<string | null>(null);

    const {
        register,
        handleSubmit,
        setValue,
        formState: { errors, isSubmitting },
    } = useForm<SignUpValues>({
        resolver: zodResolver(signUpSchema),
    });

    const onSubmit = async (data: SignUpValues) => {
        try {
            setError(null);
            // Exclude confirmPassword from API payload
            const { confirmPassword, ...apiData } = data;

            // Register & Auto-Login (Handled by Context & Backend Cookie)
            await registerUser(apiData);

            toast.success("Account created successfully!");

            // Redirect to Home
            navigate('/');
        } catch (err: any) {
            console.error("Registration failed", err);
            const errorMessage = err.response?.data?.detail || "Registration failed";
            setError(errorMessage);
            toast.error("Sign up failed", {
                description: errorMessage
            });
        }
    };

    return (
        <div className="flex h-full w-full items-center justify-center py-8">
            <Card className="w-full max-w-lg border border-white/10 bg-surface/40 backdrop-blur-xl shadow-2xl relative overflow-hidden">
                {/* Subtle sheen effect */}
                <div className="absolute inset-0 bg-gradient-to-br from-white/5 to-transparent pointer-events-none" />

                <CardHeader className="space-y-1 text-center border-b border-white/5 pb-6">
                    <CardTitle className="text-2xl font-bold tracking-tight text-white">Create your account</CardTitle>
                    <CardDescription className="text-slate-400">
                        Start your trading journey with TradeSim today
                    </CardDescription>
                </CardHeader>
                <CardContent className="pt-6">
                    <form onSubmit={handleSubmit(onSubmit)} className="space-y-4">
                        {error && (
                            <div className="p-3 rounded-md bg-rose-500/10 text-rose-400 text-sm font-medium border border-rose-500/20 flex items-center gap-2">
                                <span className="h-1.5 w-1.5 rounded-full bg-rose-500" />
                                {error}
                            </div>
                        )}

                        <div className="grid grid-cols-2 gap-4">
                            <div className="space-y-2">
                                <Label htmlFor="full_name" className="text-slate-300 font-medium">Full Name</Label>
                                <Input id="full_name" placeholder="John Doe" className="bg-slate-950/50 border-white/10 text-white focus:border-blue-500/50 focus:ring-blue-500/20" {...register('full_name')} />
                                {errors.full_name && <p className="text-rose-400 text-xs mt-1">{errors.full_name.message}</p>}
                            </div>
                            <div className="space-y-2">
                                <Label htmlFor="email" className="text-slate-300 font-medium">Email</Label>
                                <Input id="email" type="email" placeholder="name@example.com" className="bg-slate-950/50 border-white/10 text-white focus:border-blue-500/50 focus:ring-blue-500/20" {...register('email')} />
                                {errors.email && <p className="text-rose-400 text-xs mt-1">{errors.email.message}</p>}
                            </div>
                        </div>

                        <div className="grid grid-cols-2 gap-4">
                            <div className="space-y-2">
                                <Label htmlFor="password" className="text-slate-300 font-medium">Password</Label>
                                <Input id="password" type="password" className="bg-slate-950/50 border-white/10 text-white focus:border-blue-500/50 focus:ring-blue-500/20" {...register('password')} />
                                {errors.password && <p className="text-rose-400 text-xs mt-1">{errors.password.message}</p>}
                            </div>
                            <div className="space-y-2">
                                <Label htmlFor="confirmPassword" className="text-slate-300 font-medium">Confirm</Label>
                                <Input id="confirmPassword" type="password" className="bg-slate-950/50 border-white/10 text-white focus:border-blue-500/50 focus:ring-blue-500/20" {...register('confirmPassword')} />
                                {errors.confirmPassword && <p className="text-rose-400 text-xs mt-1">{errors.confirmPassword.message}</p>}
                            </div>
                        </div>

                        <div className="space-y-2">
                            <Label htmlFor="country" className="text-slate-300 font-medium">Country</Label>
                            <Select onValueChange={(val: string) => setValue("country", val)}>
                                <SelectTrigger className="bg-slate-950/50 border-white/10 text-white focus:border-blue-500/50 focus:ring-blue-500/20">
                                    <SelectValue placeholder="Select Country" />
                                </SelectTrigger>
                                <SelectContent className="bg-slate-900 border-slate-800 text-slate-200">
                                    <SelectItem value="United States">United States</SelectItem>
                                    <SelectItem value="India">India</SelectItem>
                                    <SelectItem value="United Kingdom">United Kingdom</SelectItem>
                                    <SelectItem value="Canada">Canada</SelectItem>
                                    <SelectItem value="Other">Other</SelectItem>
                                </SelectContent>
                            </Select>
                            {errors.country && <p className="text-rose-400 text-xs mt-1">{errors.country.message}</p>}
                        </div>

                        <div className="space-y-2">
                            <Label htmlFor="investment_goals" className="text-slate-300 font-medium">Investment Goals</Label>
                            <Select onValueChange={(val: string) => setValue("investment_goals", val)}>
                                <SelectTrigger className="bg-slate-950/50 border-white/10 text-white focus:border-blue-500/50 focus:ring-blue-500/20">
                                    <SelectValue placeholder="Select Goal" />
                                </SelectTrigger>
                                <SelectContent className="bg-slate-900 border-slate-800 text-slate-200">
                                    <SelectItem value="Growth">Growth</SelectItem>
                                    <SelectItem value="Value">Value</SelectItem>
                                    <SelectItem value="Day Trading">Day Trading</SelectItem>
                                    <SelectItem value="Passive Income">Passive Income</SelectItem>
                                </SelectContent>
                            </Select>
                            {errors.investment_goals && <p className="text-rose-400 text-xs mt-1">{errors.investment_goals.message}</p>}
                        </div>

                        <div className="grid grid-cols-2 gap-4">
                            <div className="space-y-2">
                                <Label htmlFor="risk_tolerance" className="text-slate-300 font-medium">Risk Tolerance</Label>
                                <Select onValueChange={(val: string) => setValue("risk_tolerance", val)}>
                                    <SelectTrigger className="bg-slate-950/50 border-white/10 text-white focus:border-blue-500/50 focus:ring-blue-500/20">
                                        <SelectValue placeholder="Select Risk" />
                                    </SelectTrigger>
                                    <SelectContent className="bg-slate-900 border-slate-800 text-slate-200">
                                        <SelectItem value="Low">Low</SelectItem>
                                        <SelectItem value="Medium">Medium</SelectItem>
                                        <SelectItem value="High">High</SelectItem>
                                    </SelectContent>
                                </Select>
                                {errors.risk_tolerance && <p className="text-rose-400 text-xs mt-1">{errors.risk_tolerance.message}</p>}
                            </div>
                            <div className="space-y-2">
                                <Label htmlFor="preferred_industries" className="text-slate-300 font-medium">Preferred Industries</Label>
                                <Select onValueChange={(val: string) => setValue("preferred_industries", val)}>
                                    <SelectTrigger className="bg-slate-950/50 border-white/10 text-white focus:border-blue-500/50 focus:ring-blue-500/20">
                                        <SelectValue placeholder="Select Industry" />
                                    </SelectTrigger>
                                    <SelectContent className="bg-slate-900 border-slate-800 text-slate-200">
                                        <SelectItem value="Technology">Technology</SelectItem>
                                        <SelectItem value="Healthcare">Healthcare</SelectItem>
                                        <SelectItem value="Finance">Finance</SelectItem>
                                        <SelectItem value="Energy">Energy</SelectItem>
                                        <SelectItem value="Consumer Goods">Consumer Goods</SelectItem>
                                    </SelectContent>
                                </Select>
                                {errors.preferred_industries && <p className="text-rose-400 text-xs mt-1">{errors.preferred_industries.message}</p>}
                            </div>
                        </div>

                        <Button
                            className="w-full mt-6 h-11 bg-gradient-to-r from-blue-600 to-blue-700 hover:from-blue-500 hover:to-blue-600 text-white shadow-lg shadow-blue-600/20 transition-all hover:scale-[1.02] active:scale-[0.98] font-medium tracking-wide"
                            type="submit"
                            disabled={isSubmitting}
                        >
                            {isSubmitting ? (
                                <div className="flex items-center gap-2">
                                    <div className="h-4 w-4 animate-spin rounded-full border-2 border-white/20 border-t-white" />
                                    <span>Creating account...</span>
                                </div>
                            ) : 'Create Account'}
                        </Button>
                    </form>
                </CardContent>
                <CardFooter className="flex justify-center border-t border-white/5 bg-surface/30 p-6">
                    <p className="text-sm text-slate-400">
                        Already have an account?{' '}
                        <Link to="/auth/sign-in" className="font-medium text-blue-400 hover:text-blue-300 hover:underline transition-colors">
                            Sign in
                        </Link>
                    </p>
                </CardFooter>
            </Card>
        </div>
    );
};

export default SignUp;
