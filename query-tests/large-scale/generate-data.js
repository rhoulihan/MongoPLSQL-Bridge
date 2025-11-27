#!/usr/bin/env node
/**
 * Large-Scale Data Generator for MongoPLSQL-Bridge Testing
 *
 * Generates ~4GB of rich, deeply nested documents across multiple collections
 * for comprehensive cross-database validation testing.
 *
 * Usage:
 *   node generate-data.js [--size small|medium|large|xlarge] [--output <dir>]
 *
 * Sizes:
 *   small  - ~100MB (for quick testing)
 *   medium - ~500MB
 *   large  - ~2GB
 *   xlarge - ~4GB (default)
 */

const fs = require('fs');
const path = require('path');

// Configuration based on size
const SIZES = {
    small: {
        ecommerce_orders: 10000,
        ecommerce_products: 1000,
        ecommerce_customers: 2000,
        ecommerce_reviews: 20000,
        analytics_events: 50000,
        analytics_sessions: 10000,
        social_users: 5000,
        social_posts: 20000,
        iot_devices: 1000,
        iot_readings: 100000
    },
    medium: {
        ecommerce_orders: 50000,
        ecommerce_products: 5000,
        ecommerce_customers: 10000,
        ecommerce_reviews: 100000,
        analytics_events: 250000,
        analytics_sessions: 50000,
        social_users: 25000,
        social_posts: 100000,
        iot_devices: 5000,
        iot_readings: 500000
    },
    large: {
        ecommerce_orders: 200000,
        ecommerce_products: 20000,
        ecommerce_customers: 50000,
        ecommerce_reviews: 500000,
        analytics_events: 1000000,
        analytics_sessions: 200000,
        social_users: 100000,
        social_posts: 500000,
        iot_devices: 20000,
        iot_readings: 2000000
    },
    xlarge: {
        ecommerce_orders: 500000,
        ecommerce_products: 50000,
        ecommerce_customers: 100000,
        ecommerce_reviews: 1000000,
        analytics_events: 2500000,
        analytics_sessions: 500000,
        social_users: 250000,
        social_posts: 1000000,
        iot_devices: 50000,
        iot_readings: 5000000
    }
};

// Helper functions
function randomInt(min, max) {
    return Math.floor(Math.random() * (max - min + 1)) + min;
}

function randomFloat(min, max, decimals = 2) {
    return parseFloat((Math.random() * (max - min) + min).toFixed(decimals));
}

function randomChoice(arr) {
    return arr[Math.floor(Math.random() * arr.length)];
}

function randomChoices(arr, count) {
    const shuffled = [...arr].sort(() => 0.5 - Math.random());
    return shuffled.slice(0, count);
}

function randomDate(startYear = 2020, endYear = 2024) {
    const start = new Date(startYear, 0, 1).getTime();
    const end = new Date(endYear, 11, 31).getTime();
    return new Date(start + Math.random() * (end - start));
}

function randomString(length) {
    const chars = 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789';
    return Array.from({ length }, () => chars[Math.floor(Math.random() * chars.length)]).join('');
}

function randomEmail(name) {
    const domains = ['gmail.com', 'yahoo.com', 'outlook.com', 'company.com', 'example.org'];
    return `${name.toLowerCase().replace(/\s+/g, '.')}${randomInt(1, 999)}@${randomChoice(domains)}`;
}

function randomPhone() {
    return `+1-${randomInt(200, 999)}-${randomInt(100, 999)}-${randomInt(1000, 9999)}`;
}

function randomAddress() {
    const streets = ['Main St', 'Oak Ave', 'Maple Dr', 'Cedar Ln', 'Pine Rd', 'Elm Blvd', 'Park Way', 'Lake View'];
    const cities = ['New York', 'Los Angeles', 'Chicago', 'Houston', 'Phoenix', 'Seattle', 'Denver', 'Boston', 'Miami', 'Atlanta'];
    const states = ['NY', 'CA', 'IL', 'TX', 'AZ', 'WA', 'CO', 'MA', 'FL', 'GA'];
    const idx = randomInt(0, cities.length - 1);
    return {
        street: `${randomInt(100, 9999)} ${randomChoice(streets)}`,
        city: cities[idx],
        state: states[idx],
        zipCode: `${randomInt(10000, 99999)}`,
        country: 'USA',
        coordinates: {
            lat: randomFloat(25.0, 48.0, 6),
            lng: randomFloat(-125.0, -70.0, 6)
        }
    };
}

// Data domains
const CATEGORIES = ['Electronics', 'Clothing', 'Home & Garden', 'Sports', 'Books', 'Toys', 'Food', 'Health', 'Automotive', 'Jewelry'];
const SUBCATEGORIES = {
    'Electronics': ['Phones', 'Laptops', 'TVs', 'Audio', 'Cameras', 'Gaming', 'Wearables', 'Accessories'],
    'Clothing': ['Men', 'Women', 'Kids', 'Shoes', 'Accessories', 'Sportswear', 'Formal', 'Casual'],
    'Home & Garden': ['Furniture', 'Kitchen', 'Bathroom', 'Garden', 'Lighting', 'Decor', 'Tools', 'Storage'],
    'Sports': ['Fitness', 'Outdoor', 'Team Sports', 'Water Sports', 'Winter Sports', 'Cycling', 'Running', 'Yoga'],
    'Books': ['Fiction', 'Non-Fiction', 'Science', 'History', 'Biography', 'Children', 'Comics', 'Education'],
    'Toys': ['Action Figures', 'Board Games', 'Dolls', 'Building', 'Educational', 'Outdoor', 'Electronic', 'Puzzles'],
    'Food': ['Snacks', 'Beverages', 'Organic', 'International', 'Gourmet', 'Diet', 'Fresh', 'Frozen'],
    'Health': ['Vitamins', 'Personal Care', 'Medical', 'Fitness', 'Supplements', 'First Aid', 'Baby', 'Senior'],
    'Automotive': ['Parts', 'Accessories', 'Tools', 'Care', 'Electronics', 'Interior', 'Exterior', 'Safety'],
    'Jewelry': ['Rings', 'Necklaces', 'Bracelets', 'Earrings', 'Watches', 'Fine', 'Fashion', 'Custom']
};
const BRANDS = ['TechPro', 'StyleMax', 'HomeEase', 'SportFit', 'QualityFirst', 'ValueBrand', 'PremiumLine', 'EcoChoice', 'SmartBuy', 'LuxurySelect'];
const PAYMENT_METHODS = ['credit_card', 'debit_card', 'paypal', 'apple_pay', 'google_pay', 'bank_transfer', 'crypto'];
const ORDER_STATUSES = ['pending', 'processing', 'shipped', 'delivered', 'cancelled', 'returned', 'refunded'];
const SHIPPING_METHODS = ['standard', 'express', 'overnight', 'pickup', 'freight'];
const FIRST_NAMES = ['James', 'Mary', 'John', 'Patricia', 'Robert', 'Jennifer', 'Michael', 'Linda', 'William', 'Elizabeth', 'David', 'Barbara', 'Richard', 'Susan', 'Joseph', 'Jessica', 'Thomas', 'Sarah', 'Charles', 'Karen'];
const LAST_NAMES = ['Smith', 'Johnson', 'Williams', 'Brown', 'Jones', 'Garcia', 'Miller', 'Davis', 'Rodriguez', 'Martinez', 'Hernandez', 'Lopez', 'Gonzalez', 'Wilson', 'Anderson', 'Thomas', 'Taylor', 'Moore', 'Jackson', 'Martin'];
const EVENT_TYPES = ['page_view', 'click', 'scroll', 'form_submit', 'purchase', 'add_to_cart', 'remove_from_cart', 'search', 'login', 'logout', 'signup', 'error'];
const DEVICE_TYPES = ['mobile', 'desktop', 'tablet', 'smart_tv', 'console'];
const BROWSERS = ['Chrome', 'Firefox', 'Safari', 'Edge', 'Opera'];
const OS_LIST = ['Windows', 'macOS', 'iOS', 'Android', 'Linux'];
const SENSOR_TYPES = ['temperature', 'humidity', 'pressure', 'motion', 'light', 'sound', 'vibration', 'gas'];

// Document generators

/**
 * E-commerce Product - Deeply nested with variants, specifications, and metadata
 */
function generateProduct(id) {
    const category = randomChoice(CATEGORIES);
    const subcategory = randomChoice(SUBCATEGORIES[category]);
    const brand = randomChoice(BRANDS);
    const basePrice = randomFloat(9.99, 999.99);

    const variants = Array.from({ length: randomInt(1, 8) }, (_, i) => ({
        sku: `${brand.substring(0, 3).toUpperCase()}-${id}-${i + 1}`,
        name: `Variant ${i + 1}`,
        attributes: {
            color: randomChoice(['Red', 'Blue', 'Green', 'Black', 'White', 'Gray', 'Navy', 'Brown']),
            size: randomChoice(['XS', 'S', 'M', 'L', 'XL', 'XXL', 'One Size']),
            material: randomChoice(['Cotton', 'Polyester', 'Leather', 'Metal', 'Plastic', 'Wood', 'Glass'])
        },
        price: randomFloat(basePrice * 0.8, basePrice * 1.5),
        costPrice: randomFloat(basePrice * 0.3, basePrice * 0.6),
        inventory: {
            quantity: randomInt(0, 1000),
            reserved: randomInt(0, 50),
            warehouse: randomChoice(['WH-EAST', 'WH-WEST', 'WH-CENTRAL', 'WH-SOUTH']),
            reorderPoint: randomInt(10, 100),
            reorderQuantity: randomInt(50, 500)
        },
        images: Array.from({ length: randomInt(1, 5) }, (_, j) => ({
            url: `https://cdn.example.com/products/${id}/variant${i + 1}/image${j + 1}.jpg`,
            alt: `Product ${id} Variant ${i + 1} Image ${j + 1}`,
            isPrimary: j === 0,
            dimensions: { width: 800, height: 800 }
        })),
        isActive: Math.random() > 0.1
    }));

    return {
        _id: id,
        name: `${brand} ${category} ${subcategory} Item ${id}`,
        slug: `${brand.toLowerCase()}-${category.toLowerCase()}-${id}`,
        description: {
            short: `High-quality ${subcategory.toLowerCase()} from ${brand}`,
            long: `This premium ${subcategory.toLowerCase()} product from ${brand} offers exceptional quality and value. Perfect for ${category.toLowerCase()} enthusiasts looking for reliable performance and style.`,
            features: Array.from({ length: randomInt(3, 8) }, (_, i) => `Feature ${i + 1}: ${randomString(20)}`),
            specifications: {
                weight: { value: randomFloat(0.1, 50, 2), unit: 'kg' },
                dimensions: {
                    length: randomFloat(5, 100, 1),
                    width: randomFloat(5, 100, 1),
                    height: randomFloat(5, 100, 1),
                    unit: 'cm'
                },
                warranty: `${randomInt(1, 5)} years`,
                origin: randomChoice(['USA', 'China', 'Germany', 'Japan', 'Italy', 'UK', 'France'])
            }
        },
        category: {
            primary: category,
            secondary: subcategory,
            tertiary: `${subcategory} - Type ${randomInt(1, 5)}`,
            breadcrumb: [category, subcategory, `Type ${randomInt(1, 5)}`]
        },
        brand: {
            name: brand,
            id: `brand-${BRANDS.indexOf(brand) + 1}`,
            logo: `https://cdn.example.com/brands/${brand.toLowerCase()}.png`,
            verified: Math.random() > 0.3
        },
        pricing: {
            basePrice: basePrice,
            currency: 'USD',
            discounts: Array.from({ length: randomInt(0, 3) }, () => ({
                type: randomChoice(['percentage', 'fixed', 'bundle']),
                value: randomFloat(5, 30),
                code: randomString(8).toUpperCase(),
                validFrom: randomDate(2024, 2024),
                validTo: randomDate(2024, 2025),
                minPurchase: randomFloat(50, 200),
                maxUses: randomInt(100, 10000)
            })),
            taxCategory: randomChoice(['standard', 'reduced', 'zero', 'exempt'])
        },
        variants: variants,
        ratings: {
            average: randomFloat(1, 5, 1),
            count: randomInt(0, 10000),
            distribution: {
                '5': randomInt(0, 5000),
                '4': randomInt(0, 3000),
                '3': randomInt(0, 1000),
                '2': randomInt(0, 500),
                '1': randomInt(0, 200)
            }
        },
        seo: {
            title: `Buy ${brand} ${subcategory} | Best ${category}`,
            description: `Shop ${brand} ${subcategory} at great prices. Free shipping on orders over $50.`,
            keywords: [brand.toLowerCase(), category.toLowerCase(), subcategory.toLowerCase(), 'buy', 'shop', 'best price'],
            canonicalUrl: `https://shop.example.com/products/${id}`
        },
        shipping: {
            weight: randomFloat(0.1, 20, 2),
            dimensions: {
                length: randomFloat(10, 100, 1),
                width: randomFloat(10, 100, 1),
                height: randomFloat(10, 100, 1)
            },
            freeShipping: Math.random() > 0.5,
            restrictions: randomChoice([null, 'fragile', 'hazmat', 'oversized', 'refrigerated'])
        },
        metadata: {
            createdAt: randomDate(2020, 2022),
            updatedAt: randomDate(2023, 2024),
            createdBy: `admin-${randomInt(1, 10)}`,
            version: randomInt(1, 20),
            status: randomChoice(['active', 'draft', 'archived', 'pending_review']),
            flags: randomChoices(['bestseller', 'new_arrival', 'clearance', 'limited_edition', 'eco_friendly', 'handmade'], randomInt(0, 3)),
            analytics: {
                views: randomInt(100, 100000),
                clicks: randomInt(50, 50000),
                conversions: randomInt(10, 10000),
                revenue: randomFloat(1000, 1000000)
            }
        },
        relatedProducts: Array.from({ length: randomInt(3, 10) }, () => randomInt(1, 50000)),
        tags: randomChoices(['popular', 'trending', 'sale', 'new', 'exclusive', 'premium', 'value', 'eco', 'gift', 'seasonal'], randomInt(2, 6))
    };
}

/**
 * E-commerce Customer - With addresses, preferences, and history
 */
function generateCustomer(id) {
    const firstName = randomChoice(FIRST_NAMES);
    const lastName = randomChoice(LAST_NAMES);
    const fullName = `${firstName} ${lastName}`;
    const registrationDate = randomDate(2018, 2023);

    return {
        _id: id,
        profile: {
            firstName: firstName,
            lastName: lastName,
            fullName: fullName,
            email: randomEmail(fullName),
            phone: randomPhone(),
            dateOfBirth: randomDate(1950, 2005),
            gender: randomChoice(['male', 'female', 'other', 'prefer_not_to_say']),
            avatar: `https://cdn.example.com/avatars/${id}.jpg`
        },
        addresses: Array.from({ length: randomInt(1, 4) }, (_, i) => ({
            id: `addr-${id}-${i + 1}`,
            type: i === 0 ? 'primary' : randomChoice(['shipping', 'billing', 'work']),
            ...randomAddress(),
            isDefault: i === 0,
            label: randomChoice(['Home', 'Work', 'Parents', 'Partner', 'Other'])
        })),
        preferences: {
            language: randomChoice(['en', 'es', 'fr', 'de', 'zh', 'ja']),
            currency: randomChoice(['USD', 'EUR', 'GBP', 'CAD', 'AUD']),
            timezone: randomChoice(['America/New_York', 'America/Los_Angeles', 'Europe/London', 'Asia/Tokyo']),
            notifications: {
                email: Math.random() > 0.3,
                sms: Math.random() > 0.5,
                push: Math.random() > 0.4,
                frequency: randomChoice(['immediate', 'daily', 'weekly', 'never'])
            },
            marketing: {
                optIn: Math.random() > 0.4,
                categories: randomChoices(CATEGORIES, randomInt(1, 5)),
                channels: randomChoices(['email', 'sms', 'social', 'mail'], randomInt(1, 3))
            }
        },
        loyalty: {
            tier: randomChoice(['bronze', 'silver', 'gold', 'platinum', 'diamond']),
            points: randomInt(0, 100000),
            lifetimePoints: randomInt(1000, 500000),
            memberSince: registrationDate,
            benefits: randomChoices(['free_shipping', 'early_access', 'exclusive_deals', 'birthday_bonus', 'points_multiplier'], randomInt(1, 4))
        },
        paymentMethods: Array.from({ length: randomInt(1, 4) }, (_, i) => ({
            id: `pm-${id}-${i + 1}`,
            type: randomChoice(PAYMENT_METHODS),
            last4: `${randomInt(1000, 9999)}`,
            brand: randomChoice(['Visa', 'Mastercard', 'Amex', 'Discover']),
            expiryMonth: randomInt(1, 12),
            expiryYear: randomInt(2024, 2030),
            isDefault: i === 0,
            billingAddress: randomAddress()
        })),
        orderHistory: {
            totalOrders: randomInt(1, 200),
            totalSpent: randomFloat(100, 50000),
            averageOrderValue: randomFloat(50, 500),
            lastOrderDate: randomDate(2023, 2024),
            favoriteCategories: randomChoices(CATEGORIES, randomInt(1, 4)),
            returnRate: randomFloat(0, 0.3, 3)
        },
        segments: randomChoices(['high_value', 'frequent_buyer', 'deal_seeker', 'brand_loyal', 'new_customer', 'at_risk', 'churned', 'reactivated'], randomInt(1, 3)),
        metadata: {
            registrationDate: registrationDate,
            lastLoginDate: randomDate(2024, 2024),
            loginCount: randomInt(1, 500),
            source: randomChoice(['organic', 'referral', 'paid_search', 'social', 'affiliate', 'email']),
            device: randomChoice(DEVICE_TYPES),
            verified: Math.random() > 0.2,
            status: randomChoice(['active', 'inactive', 'suspended', 'deleted'])
        }
    };
}

/**
 * E-commerce Order - Complex with line items, shipping, and payment details
 */
function generateOrder(id, customerCount, productCount) {
    const customerId = randomInt(1, customerCount);
    const orderDate = randomDate(2022, 2024);
    const itemCount = randomInt(1, 10);

    const items = Array.from({ length: itemCount }, (_, i) => {
        const productId = randomInt(1, productCount);
        const quantity = randomInt(1, 5);
        const unitPrice = randomFloat(9.99, 499.99);
        const discount = Math.random() > 0.7 ? randomFloat(5, 30) : 0;

        return {
            lineId: `line-${id}-${i + 1}`,
            productId: productId,
            variantSku: `SKU-${productId}-${randomInt(1, 5)}`,
            name: `Product ${productId} - Variant ${randomInt(1, 5)}`,
            quantity: quantity,
            unitPrice: unitPrice,
            discount: {
                type: discount > 0 ? randomChoice(['percentage', 'fixed']) : null,
                value: discount,
                code: discount > 0 ? randomString(8).toUpperCase() : null
            },
            tax: {
                rate: randomFloat(0, 0.12, 4),
                amount: unitPrice * quantity * randomFloat(0, 0.12, 4)
            },
            subtotal: unitPrice * quantity,
            total: unitPrice * quantity * (1 - discount / 100),
            fulfillment: {
                status: randomChoice(['pending', 'picked', 'packed', 'shipped', 'delivered']),
                warehouse: randomChoice(['WH-EAST', 'WH-WEST', 'WH-CENTRAL']),
                trackingNumber: Math.random() > 0.3 ? randomString(20).toUpperCase() : null
            },
            returnInfo: Math.random() > 0.9 ? {
                requested: true,
                reason: randomChoice(['defective', 'wrong_item', 'not_as_described', 'changed_mind', 'too_late']),
                status: randomChoice(['pending', 'approved', 'rejected', 'completed']),
                refundAmount: unitPrice * quantity * randomFloat(0.5, 1)
            } : null
        };
    });

    const subtotal = items.reduce((sum, item) => sum + item.subtotal, 0);
    const totalDiscount = items.reduce((sum, item) => sum + (item.discount.value || 0) * item.subtotal / 100, 0);
    const totalTax = items.reduce((sum, item) => sum + item.tax.amount, 0);
    const shippingCost = randomFloat(0, 25.99);

    return {
        _id: id,
        orderNumber: `ORD-${orderDate.getFullYear()}-${String(id).padStart(8, '0')}`,
        customerId: customerId,
        status: randomChoice(ORDER_STATUSES),
        items: items,
        pricing: {
            subtotal: subtotal,
            discount: totalDiscount,
            tax: totalTax,
            shipping: shippingCost,
            total: subtotal - totalDiscount + totalTax + shippingCost,
            currency: 'USD'
        },
        shipping: {
            method: randomChoice(SHIPPING_METHODS),
            address: randomAddress(),
            carrier: randomChoice(['UPS', 'FedEx', 'USPS', 'DHL', 'Amazon']),
            trackingNumber: randomString(20).toUpperCase(),
            estimatedDelivery: new Date(orderDate.getTime() + randomInt(1, 14) * 24 * 60 * 60 * 1000),
            actualDelivery: Math.random() > 0.3 ? new Date(orderDate.getTime() + randomInt(1, 14) * 24 * 60 * 60 * 1000) : null,
            instructions: Math.random() > 0.7 ? randomChoice(['Leave at door', 'Signature required', 'Call before delivery', 'Leave with neighbor']) : null
        },
        payment: {
            method: randomChoice(PAYMENT_METHODS),
            status: randomChoice(['pending', 'authorized', 'captured', 'failed', 'refunded']),
            transactionId: `txn-${randomString(16)}`,
            last4: `${randomInt(1000, 9999)}`,
            brand: randomChoice(['Visa', 'Mastercard', 'Amex']),
            billingAddress: randomAddress(),
            attempts: Array.from({ length: randomInt(1, 3) }, () => ({
                timestamp: randomDate(2024, 2024),
                status: randomChoice(['success', 'failed', 'pending']),
                errorCode: Math.random() > 0.8 ? randomChoice(['insufficient_funds', 'card_declined', 'expired', 'invalid_cvv']) : null
            }))
        },
        promotions: Math.random() > 0.5 ? Array.from({ length: randomInt(1, 3) }, () => ({
            code: randomString(8).toUpperCase(),
            type: randomChoice(['percentage', 'fixed', 'free_shipping', 'bogo']),
            value: randomFloat(5, 50),
            applied: randomFloat(5, 100)
        })) : [],
        notes: {
            customer: Math.random() > 0.8 ? `Customer note: ${randomString(50)}` : null,
            internal: Math.random() > 0.7 ? `Internal note: ${randomString(30)}` : null,
            gift: Math.random() > 0.9 ? { isGift: true, message: randomString(100), wrap: true } : null
        },
        timestamps: {
            createdAt: orderDate,
            updatedAt: new Date(orderDate.getTime() + randomInt(0, 7) * 24 * 60 * 60 * 1000),
            processedAt: new Date(orderDate.getTime() + randomInt(0, 2) * 24 * 60 * 60 * 1000),
            shippedAt: Math.random() > 0.3 ? new Date(orderDate.getTime() + randomInt(1, 5) * 24 * 60 * 60 * 1000) : null,
            deliveredAt: Math.random() > 0.5 ? new Date(orderDate.getTime() + randomInt(3, 14) * 24 * 60 * 60 * 1000) : null
        },
        source: {
            channel: randomChoice(['web', 'mobile_app', 'phone', 'in_store', 'marketplace']),
            platform: randomChoice(['ios', 'android', 'web', 'pos']),
            referrer: Math.random() > 0.5 ? randomChoice(['google', 'facebook', 'instagram', 'email', 'direct']) : null,
            campaign: Math.random() > 0.7 ? `campaign-${randomString(10)}` : null
        },
        fraud: {
            score: randomFloat(0, 100),
            flags: randomChoices(['high_value', 'new_customer', 'different_shipping', 'multiple_cards', 'velocity'], randomInt(0, 2)),
            reviewed: Math.random() > 0.9,
            approved: Math.random() > 0.05
        }
    };
}

/**
 * Product Review - With nested ratings and helpful votes
 */
function generateReview(id, productCount, customerCount) {
    const reviewDate = randomDate(2020, 2024);

    return {
        _id: id,
        productId: randomInt(1, productCount),
        customerId: randomInt(1, customerCount),
        orderId: Math.random() > 0.2 ? randomInt(1, productCount * 2) : null,
        verified: Math.random() > 0.3,
        rating: {
            overall: randomInt(1, 5),
            aspects: {
                quality: randomInt(1, 5),
                value: randomInt(1, 5),
                shipping: randomInt(1, 5),
                packaging: randomInt(1, 5)
            }
        },
        title: `Review Title ${id}`,
        content: {
            text: `This is a detailed review for product. ${randomString(200)}`,
            pros: Array.from({ length: randomInt(1, 5) }, () => randomString(20)),
            cons: Array.from({ length: randomInt(0, 3) }, () => randomString(20))
        },
        media: Math.random() > 0.7 ? Array.from({ length: randomInt(1, 5) }, (_, i) => ({
            type: randomChoice(['image', 'video']),
            url: `https://cdn.example.com/reviews/${id}/media${i + 1}.jpg`,
            thumbnail: `https://cdn.example.com/reviews/${id}/thumb${i + 1}.jpg`,
            caption: Math.random() > 0.5 ? randomString(30) : null
        })) : [],
        helpful: {
            upvotes: randomInt(0, 500),
            downvotes: randomInt(0, 50),
            reports: randomInt(0, 5)
        },
        response: Math.random() > 0.8 ? {
            author: `Support Team`,
            content: `Thank you for your feedback. ${randomString(100)}`,
            timestamp: new Date(reviewDate.getTime() + randomInt(1, 7) * 24 * 60 * 60 * 1000)
        } : null,
        metadata: {
            createdAt: reviewDate,
            updatedAt: Math.random() > 0.8 ? new Date(reviewDate.getTime() + randomInt(1, 30) * 24 * 60 * 60 * 1000) : reviewDate,
            status: randomChoice(['published', 'pending', 'flagged', 'removed']),
            source: randomChoice(['web', 'mobile', 'email_request']),
            language: randomChoice(['en', 'es', 'fr', 'de']),
            sentiment: randomChoice(['positive', 'neutral', 'negative', 'mixed'])
        }
    };
}

/**
 * Analytics Event - Web/app tracking with nested context
 */
function generateAnalyticsEvent(id, sessionCount) {
    const eventDate = randomDate(2024, 2024);

    return {
        _id: id,
        sessionId: randomInt(1, sessionCount),
        eventType: randomChoice(EVENT_TYPES),
        timestamp: eventDate,
        user: {
            id: Math.random() > 0.3 ? `user-${randomInt(1, 100000)}` : null,
            anonymousId: `anon-${randomString(32)}`,
            traits: {
                email: Math.random() > 0.5 ? randomEmail('user') : null,
                plan: randomChoice(['free', 'basic', 'pro', 'enterprise']),
                createdAt: randomDate(2020, 2024)
            }
        },
        context: {
            page: {
                path: `/${randomChoice(['home', 'products', 'cart', 'checkout', 'account', 'search', 'category'])}`,
                title: `Page Title ${randomInt(1, 100)}`,
                url: `https://shop.example.com/${randomString(20)}`,
                referrer: Math.random() > 0.4 ? `https://${randomChoice(['google.com', 'facebook.com', 'twitter.com'])}` : null
            },
            device: {
                type: randomChoice(DEVICE_TYPES),
                manufacturer: randomChoice(['Apple', 'Samsung', 'Google', 'Dell', 'HP']),
                model: randomString(10),
                screenResolution: randomChoice(['1920x1080', '2560x1440', '1366x768', '3840x2160', '750x1334'])
            },
            browser: {
                name: randomChoice(BROWSERS),
                version: `${randomInt(80, 120)}.0.${randomInt(1000, 9999)}`,
                userAgent: `Mozilla/5.0 ${randomString(50)}`
            },
            os: {
                name: randomChoice(OS_LIST),
                version: `${randomInt(10, 15)}.${randomInt(0, 9)}`
            },
            location: {
                country: randomChoice(['US', 'UK', 'CA', 'DE', 'FR', 'JP', 'AU']),
                region: randomString(10),
                city: randomChoice(['New York', 'London', 'Tokyo', 'Paris', 'Sydney']),
                ip: `${randomInt(1, 255)}.${randomInt(0, 255)}.${randomInt(0, 255)}.${randomInt(0, 255)}`
            },
            campaign: Math.random() > 0.6 ? {
                source: randomChoice(['google', 'facebook', 'email', 'affiliate']),
                medium: randomChoice(['cpc', 'organic', 'referral', 'social']),
                name: `campaign-${randomString(10)}`,
                content: randomString(20)
            } : null
        },
        properties: {
            category: randomChoice(CATEGORIES),
            productId: Math.random() > 0.5 ? randomInt(1, 50000) : null,
            value: Math.random() > 0.3 ? randomFloat(1, 1000) : null,
            quantity: Math.random() > 0.5 ? randomInt(1, 10) : null,
            searchQuery: Math.random() > 0.8 ? randomString(20) : null,
            errorMessage: Math.random() > 0.95 ? `Error: ${randomString(30)}` : null,
            custom: {
                experimentId: Math.random() > 0.7 ? `exp-${randomInt(1, 100)}` : null,
                variant: Math.random() > 0.7 ? randomChoice(['control', 'treatment_a', 'treatment_b']) : null,
                featureFlags: randomChoices(['new_checkout', 'recommendations_v2', 'dark_mode', 'ai_search'], randomInt(0, 3))
            }
        },
        performance: {
            loadTime: randomInt(100, 5000),
            domReady: randomInt(50, 2000),
            firstPaint: randomInt(50, 1000),
            firstContentfulPaint: randomInt(100, 2000)
        }
    };
}

/**
 * Analytics Session - Aggregated session data
 */
function generateSession(id) {
    const sessionStart = randomDate(2024, 2024);
    const duration = randomInt(10, 3600);

    return {
        _id: id,
        sessionId: `sess-${randomString(32)}`,
        userId: Math.random() > 0.3 ? `user-${randomInt(1, 100000)}` : null,
        anonymousId: `anon-${randomString(32)}`,
        startTime: sessionStart,
        endTime: new Date(sessionStart.getTime() + duration * 1000),
        duration: duration,
        device: {
            type: randomChoice(DEVICE_TYPES),
            browser: randomChoice(BROWSERS),
            os: randomChoice(OS_LIST),
            isMobile: Math.random() > 0.5
        },
        location: {
            country: randomChoice(['US', 'UK', 'CA', 'DE', 'FR', 'JP', 'AU']),
            city: randomChoice(['New York', 'London', 'Tokyo', 'Paris', 'Sydney']),
            timezone: randomChoice(['America/New_York', 'Europe/London', 'Asia/Tokyo'])
        },
        traffic: {
            source: randomChoice(['direct', 'organic', 'paid', 'social', 'referral', 'email']),
            medium: randomChoice(['none', 'cpc', 'organic', 'referral', 'social']),
            campaign: Math.random() > 0.6 ? `campaign-${randomInt(1, 100)}` : null,
            landingPage: `/${randomChoice(['', 'products', 'sale', 'category/electronics'])}`
        },
        engagement: {
            pageViews: randomInt(1, 50),
            uniquePages: randomInt(1, 20),
            events: randomInt(1, 100),
            clicks: randomInt(0, 200),
            scrollDepth: randomInt(0, 100),
            timeOnSite: duration
        },
        conversion: {
            converted: Math.random() > 0.9,
            revenue: Math.random() > 0.9 ? randomFloat(10, 500) : 0,
            transactions: Math.random() > 0.9 ? randomInt(1, 3) : 0,
            goals: randomChoices(['signup', 'add_to_cart', 'checkout_start', 'purchase', 'newsletter'], randomInt(0, 3))
        },
        bounced: Math.random() > 0.6,
        isNewUser: Math.random() > 0.7
    };
}

/**
 * Social Media User - With followers, preferences, and activity
 */
function generateSocialUser(id) {
    const firstName = randomChoice(FIRST_NAMES);
    const lastName = randomChoice(LAST_NAMES);
    const username = `${firstName.toLowerCase()}${lastName.toLowerCase()}${randomInt(1, 999)}`;
    const joinDate = randomDate(2015, 2023);

    return {
        _id: id,
        username: username,
        displayName: `${firstName} ${lastName}`,
        email: randomEmail(`${firstName} ${lastName}`),
        profile: {
            bio: randomString(150),
            location: randomAddress().city,
            website: Math.random() > 0.5 ? `https://${username}.com` : null,
            avatar: `https://cdn.example.com/avatars/${id}.jpg`,
            cover: Math.random() > 0.3 ? `https://cdn.example.com/covers/${id}.jpg` : null,
            verified: Math.random() > 0.95,
            private: Math.random() > 0.8
        },
        stats: {
            followers: randomInt(0, 1000000),
            following: randomInt(0, 5000),
            posts: randomInt(0, 10000),
            likes: randomInt(0, 100000),
            comments: randomInt(0, 50000),
            shares: randomInt(0, 20000)
        },
        settings: {
            notifications: {
                likes: Math.random() > 0.3,
                comments: Math.random() > 0.3,
                follows: Math.random() > 0.4,
                mentions: Math.random() > 0.2,
                messages: Math.random() > 0.2
            },
            privacy: {
                showEmail: Math.random() > 0.8,
                showLocation: Math.random() > 0.5,
                allowMessages: randomChoice(['everyone', 'followers', 'none']),
                allowTags: randomChoice(['everyone', 'followers', 'none'])
            },
            display: {
                theme: randomChoice(['light', 'dark', 'auto']),
                language: randomChoice(['en', 'es', 'fr', 'de', 'ja']),
                timezone: randomChoice(['America/New_York', 'Europe/London', 'Asia/Tokyo'])
            }
        },
        interests: randomChoices(['technology', 'sports', 'music', 'art', 'travel', 'food', 'fashion', 'gaming', 'fitness', 'photography'], randomInt(2, 6)),
        connections: {
            followers: Array.from({ length: Math.min(randomInt(0, 100), 100) }, () => randomInt(1, 250000)),
            following: Array.from({ length: Math.min(randomInt(0, 100), 100) }, () => randomInt(1, 250000)),
            blocked: Array.from({ length: randomInt(0, 10) }, () => randomInt(1, 250000)),
            muted: Array.from({ length: randomInt(0, 20) }, () => randomInt(1, 250000))
        },
        activity: {
            lastLogin: randomDate(2024, 2024),
            lastPost: randomDate(2024, 2024),
            loginStreak: randomInt(0, 365),
            totalTimeSpent: randomInt(1000, 1000000)
        },
        metadata: {
            createdAt: joinDate,
            updatedAt: randomDate(2024, 2024),
            status: randomChoice(['active', 'suspended', 'deactivated']),
            source: randomChoice(['organic', 'referral', 'ad', 'influencer']),
            deviceHistory: Array.from({ length: randomInt(1, 5) }, () => ({
                type: randomChoice(DEVICE_TYPES),
                os: randomChoice(OS_LIST),
                lastUsed: randomDate(2024, 2024)
            }))
        }
    };
}

/**
 * Social Media Post - With nested comments, reactions, and media
 */
function generateSocialPost(id, userCount) {
    const postDate = randomDate(2023, 2024);
    const authorId = randomInt(1, userCount);

    const generateComment = (depth = 0) => {
        const comment = {
            id: `comment-${randomString(12)}`,
            authorId: randomInt(1, userCount),
            content: randomString(randomInt(10, 200)),
            createdAt: new Date(postDate.getTime() + randomInt(1, 30) * 24 * 60 * 60 * 1000),
            reactions: {
                like: randomInt(0, 100),
                love: randomInt(0, 50),
                laugh: randomInt(0, 30),
                angry: randomInt(0, 10)
            },
            edited: Math.random() > 0.9
        };

        if (depth < 2 && Math.random() > 0.7) {
            comment.replies = Array.from({ length: randomInt(1, 5) }, () => generateComment(depth + 1));
        }

        return comment;
    };

    return {
        _id: id,
        authorId: authorId,
        type: randomChoice(['text', 'image', 'video', 'link', 'poll', 'story']),
        content: {
            text: randomString(randomInt(10, 500)),
            mentions: Array.from({ length: randomInt(0, 5) }, () => ({
                userId: randomInt(1, userCount),
                position: randomInt(0, 100)
            })),
            hashtags: Array.from({ length: randomInt(0, 10) }, () => `#${randomString(10)}`),
            links: Math.random() > 0.7 ? Array.from({ length: randomInt(1, 3) }, () => ({
                url: `https://${randomString(10)}.com/${randomString(20)}`,
                title: randomString(30),
                description: randomString(100),
                thumbnail: `https://cdn.example.com/links/${randomString(10)}.jpg`
            })) : []
        },
        media: Math.random() > 0.4 ? Array.from({ length: randomInt(1, 10) }, (_, i) => ({
            type: randomChoice(['image', 'video', 'gif']),
            url: `https://cdn.example.com/posts/${id}/media${i + 1}.jpg`,
            thumbnail: `https://cdn.example.com/posts/${id}/thumb${i + 1}.jpg`,
            dimensions: { width: randomInt(400, 1920), height: randomInt(400, 1080) },
            duration: Math.random() > 0.7 ? randomInt(5, 300) : null,
            alt: randomString(50)
        })) : [],
        poll: Math.random() > 0.9 ? {
            question: `${randomString(50)}?`,
            options: Array.from({ length: randomInt(2, 6) }, (_, i) => ({
                id: `option-${i + 1}`,
                text: randomString(20),
                votes: randomInt(0, 10000)
            })),
            endTime: new Date(postDate.getTime() + randomInt(1, 7) * 24 * 60 * 60 * 1000),
            multipleChoice: Math.random() > 0.7
        } : null,
        reactions: {
            like: randomInt(0, 100000),
            love: randomInt(0, 50000),
            laugh: randomInt(0, 20000),
            wow: randomInt(0, 10000),
            sad: randomInt(0, 5000),
            angry: randomInt(0, 2000)
        },
        comments: Array.from({ length: randomInt(0, 50) }, () => generateComment()),
        shares: {
            count: randomInt(0, 10000),
            users: Array.from({ length: Math.min(randomInt(0, 20), 20) }, () => randomInt(1, userCount))
        },
        engagement: {
            views: randomInt(0, 1000000),
            reach: randomInt(0, 500000),
            impressions: randomInt(0, 2000000),
            saves: randomInt(0, 50000),
            clicks: randomInt(0, 100000)
        },
        visibility: randomChoice(['public', 'followers', 'friends', 'private']),
        location: Math.random() > 0.7 ? {
            name: `${randomChoice(['New York', 'Los Angeles', 'Chicago', 'Miami', 'Seattle'])} - ${randomString(10)}`,
            coordinates: {
                lat: randomFloat(25.0, 48.0, 6),
                lng: randomFloat(-125.0, -70.0, 6)
            }
        } : null,
        metadata: {
            createdAt: postDate,
            updatedAt: Math.random() > 0.8 ? new Date(postDate.getTime() + randomInt(1, 30) * 24 * 60 * 60 * 1000) : postDate,
            edited: Math.random() > 0.9,
            pinned: Math.random() > 0.95,
            sponsored: Math.random() > 0.98,
            status: randomChoice(['published', 'draft', 'archived', 'flagged']),
            scheduledFor: Math.random() > 0.95 ? randomDate(2024, 2025) : null
        },
        moderation: {
            reviewed: Math.random() > 0.9,
            flags: randomInt(0, 10),
            appeals: randomInt(0, 2),
            status: randomChoice(['approved', 'pending', 'rejected', 'appealed'])
        }
    };
}

/**
 * IoT Device - With configuration and status
 */
function generateIoTDevice(id) {
    const installDate = randomDate(2020, 2024);

    return {
        _id: id,
        deviceId: `DEV-${randomString(12).toUpperCase()}`,
        name: `Device ${id}`,
        type: randomChoice(['sensor', 'actuator', 'gateway', 'controller', 'camera']),
        manufacturer: randomChoice(['Siemens', 'Honeywell', 'Bosch', 'ABB', 'Schneider']),
        model: `Model-${randomString(6).toUpperCase()}`,
        firmware: {
            version: `${randomInt(1, 5)}.${randomInt(0, 9)}.${randomInt(0, 99)}`,
            lastUpdate: randomDate(2024, 2024),
            autoUpdate: Math.random() > 0.5
        },
        location: {
            building: `Building ${randomChoice(['A', 'B', 'C', 'D', 'E'])}`,
            floor: randomInt(1, 20),
            room: `Room ${randomInt(100, 999)}`,
            zone: randomChoice(['North', 'South', 'East', 'West', 'Central']),
            coordinates: {
                lat: randomFloat(25.0, 48.0, 6),
                lng: randomFloat(-125.0, -70.0, 6)
            }
        },
        sensors: Array.from({ length: randomInt(1, 8) }, (_, i) => ({
            id: `sensor-${id}-${i + 1}`,
            type: randomChoice(SENSOR_TYPES),
            unit: randomChoice(['°C', '°F', '%', 'Pa', 'lux', 'dB', 'ppm', 'm/s']),
            range: { min: randomFloat(-50, 0), max: randomFloat(50, 200) },
            accuracy: randomFloat(0.1, 2, 2),
            calibrationDate: randomDate(2023, 2024)
        })),
        configuration: {
            samplingRate: randomInt(1, 60),
            transmissionInterval: randomInt(60, 3600),
            alertThresholds: {
                temperature: { min: randomFloat(-10, 10), max: randomFloat(30, 50) },
                humidity: { min: randomFloat(20, 40), max: randomFloat(60, 90) }
            },
            powerMode: randomChoice(['normal', 'eco', 'performance', 'sleep']),
            networkConfig: {
                protocol: randomChoice(['mqtt', 'http', 'coap', 'websocket']),
                endpoint: `https://iot.example.com/devices/${id}`,
                qos: randomInt(0, 2),
                encryption: Math.random() > 0.2
            }
        },
        status: {
            online: Math.random() > 0.1,
            lastSeen: randomDate(2024, 2024),
            battery: Math.random() > 0.5 ? randomInt(0, 100) : null,
            signalStrength: randomInt(-100, -30),
            health: randomChoice(['healthy', 'degraded', 'critical', 'offline']),
            errors: Array.from({ length: randomInt(0, 5) }, () => ({
                code: `ERR-${randomInt(100, 999)}`,
                message: randomString(30),
                timestamp: randomDate(2024, 2024),
                severity: randomChoice(['info', 'warning', 'error', 'critical'])
            }))
        },
        maintenance: {
            lastService: randomDate(2023, 2024),
            nextService: randomDate(2024, 2025),
            warrantyExpiry: new Date(installDate.getTime() + 2 * 365 * 24 * 60 * 60 * 1000),
            serviceHistory: Array.from({ length: randomInt(0, 5) }, () => ({
                date: randomDate(2020, 2024),
                type: randomChoice(['inspection', 'repair', 'calibration', 'replacement']),
                technician: `Tech-${randomInt(1, 100)}`,
                notes: randomString(50)
            }))
        },
        metadata: {
            installedAt: installDate,
            createdAt: installDate,
            updatedAt: randomDate(2024, 2024),
            tags: randomChoices(['critical', 'monitored', 'external', 'internal', 'production', 'test'], randomInt(1, 4)),
            owner: `dept-${randomInt(1, 20)}`,
            costCenter: `CC-${randomInt(1000, 9999)}`
        }
    };
}

/**
 * IoT Reading - Time-series sensor data
 */
function generateIoTReading(id, deviceCount) {
    const readingTime = randomDate(2024, 2024);
    const deviceId = randomInt(1, deviceCount);

    return {
        _id: id,
        deviceId: deviceId,
        timestamp: readingTime,
        readings: {
            temperature: {
                value: randomFloat(-20, 50, 2),
                unit: '°C',
                quality: randomChoice(['good', 'uncertain', 'bad'])
            },
            humidity: {
                value: randomFloat(0, 100, 1),
                unit: '%',
                quality: randomChoice(['good', 'uncertain', 'bad'])
            },
            pressure: {
                value: randomFloat(900, 1100, 1),
                unit: 'hPa',
                quality: randomChoice(['good', 'uncertain', 'bad'])
            },
            light: Math.random() > 0.5 ? {
                value: randomInt(0, 100000),
                unit: 'lux',
                quality: randomChoice(['good', 'uncertain', 'bad'])
            } : null,
            motion: Math.random() > 0.7 ? {
                detected: Math.random() > 0.5,
                intensity: randomFloat(0, 10, 2)
            } : null,
            custom: Math.random() > 0.6 ? {
                [`sensor_${randomInt(1, 5)}`]: randomFloat(0, 100, 3)
            } : null
        },
        aggregations: {
            hourly: {
                min: randomFloat(-25, 45, 2),
                max: randomFloat(-15, 55, 2),
                avg: randomFloat(-20, 50, 2),
                stddev: randomFloat(0.1, 5, 3)
            }
        },
        alerts: Math.random() > 0.9 ? Array.from({ length: randomInt(1, 3) }, () => ({
            type: randomChoice(['threshold', 'rate_of_change', 'anomaly']),
            sensor: randomChoice(SENSOR_TYPES),
            severity: randomChoice(['info', 'warning', 'critical']),
            message: randomString(50),
            acknowledged: Math.random() > 0.5
        })) : [],
        metadata: {
            batchId: `batch-${randomString(8)}`,
            transmissionDelay: randomInt(0, 5000),
            retries: randomInt(0, 3),
            compressed: Math.random() > 0.5
        }
    };
}

// Main generator
async function generateData(size, outputDir) {
    const config = SIZES[size];
    if (!config) {
        console.error(`Invalid size: ${size}. Use small, medium, large, or xlarge`);
        process.exit(1);
    }

    console.log(`Generating ${size} dataset...`);
    console.log(`Output directory: ${outputDir}`);

    if (!fs.existsSync(outputDir)) {
        fs.mkdirSync(outputDir, { recursive: true });
    }

    const collections = [
        { name: 'ecommerce_products', count: config.ecommerce_products, generator: (id) => generateProduct(id) },
        { name: 'ecommerce_customers', count: config.ecommerce_customers, generator: (id) => generateCustomer(id) },
        { name: 'ecommerce_orders', count: config.ecommerce_orders, generator: (id) => generateOrder(id, config.ecommerce_customers, config.ecommerce_products) },
        { name: 'ecommerce_reviews', count: config.ecommerce_reviews, generator: (id) => generateReview(id, config.ecommerce_products, config.ecommerce_customers) },
        { name: 'analytics_sessions', count: config.analytics_sessions, generator: (id) => generateSession(id) },
        { name: 'analytics_events', count: config.analytics_events, generator: (id) => generateAnalyticsEvent(id, config.analytics_sessions) },
        { name: 'social_users', count: config.social_users, generator: (id) => generateSocialUser(id) },
        { name: 'social_posts', count: config.social_posts, generator: (id) => generateSocialPost(id, config.social_users) },
        { name: 'iot_devices', count: config.iot_devices, generator: (id) => generateIoTDevice(id) },
        { name: 'iot_readings', count: config.iot_readings, generator: (id) => generateIoTReading(id, config.iot_devices) }
    ];

    const BATCH_SIZE = 10000;

    for (const collection of collections) {
        console.log(`\nGenerating ${collection.name}: ${collection.count.toLocaleString()} documents...`);

        const collectionDir = path.join(outputDir, collection.name);
        if (!fs.existsSync(collectionDir)) {
            fs.mkdirSync(collectionDir, { recursive: true });
        }

        let batchNum = 0;
        let documents = [];

        for (let i = 1; i <= collection.count; i++) {
            documents.push(collection.generator(i));

            if (documents.length >= BATCH_SIZE || i === collection.count) {
                batchNum++;
                const filename = path.join(collectionDir, `batch_${String(batchNum).padStart(5, '0')}.json`);
                fs.writeFileSync(filename, JSON.stringify(documents, null, 0));

                const progress = ((i / collection.count) * 100).toFixed(1);
                process.stdout.write(`\r  Progress: ${progress}% (${i.toLocaleString()}/${collection.count.toLocaleString()}) - Batch ${batchNum}`);

                documents = [];
            }
        }
        console.log(' - Done!');
    }

    // Generate manifest
    const manifest = {
        generated: new Date().toISOString(),
        size: size,
        collections: collections.map(c => ({
            name: c.name,
            count: c.count
        }))
    };
    fs.writeFileSync(path.join(outputDir, 'manifest.json'), JSON.stringify(manifest, null, 2));

    console.log('\n✅ Data generation complete!');
    console.log(`Manifest saved to: ${path.join(outputDir, 'manifest.json')}`);
}

// Parse arguments
const args = process.argv.slice(2);
let size = 'xlarge';
let outputDir = path.join(__dirname, 'data');

for (let i = 0; i < args.length; i++) {
    if (args[i] === '--size' && args[i + 1]) {
        size = args[i + 1];
        i++;
    } else if (args[i] === '--output' && args[i + 1]) {
        outputDir = args[i + 1];
        i++;
    }
}

generateData(size, outputDir);
