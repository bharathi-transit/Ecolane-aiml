# Power BI Service Principal Setup Guide

## Problem
You're receiving a `401 Unauthorized` error when trying to execute queries against the Power BI dataset. This indicates that while authentication is successful, the Service Principal lacks the necessary permissions.

## Solution Steps

### 1. Enable Service Principal in Power BI Tenant Settings

1. Go to **Power BI Admin Portal** (https://app.powerbi.com/admin-portal/tenantSettings)
2. Navigate to **Developer settings**
3. Find **"Allow service principals to use Power BI APIs"**
4. **Enable** this setting
5. Choose one of the following:
   - Apply to **Entire organization**, OR
   - Apply to **Specific security groups** (add your Service Principal to that group)
6. Click **Apply**

### 2. Add Service Principal to Workspace

1. Go to your Power BI workspace:
   - Workspace ID: `1aca1682-7521-4463-a41e-346ce2739fdf`
2. Click **Workspace Settings** (gear icon)
3. Click **Access** or **Manage access**
4. Click **Add people or groups**
5. Search for your Service Principal by **Client ID**: `4ad7f68d-7193-4743-bb33-98640ef42841`
   - Note: It may appear as the app name you registered in Azure AD
6. Assign the role: **Member** or **Contributor** (required for API access)
7. Click **Add**

### 3. Grant Dataset Permissions (If Needed)

Some datasets require explicit permissions:

1. In the workspace, find your dataset (ID: `be55c90b-3176-4b33-b2b3-cf78aa10dab7`)
2. Click **More options** (...) on the dataset
3. Click **Manage permissions**
4. Add your Service Principal with **Build** or **Read** permission
5. Click **Grant access**

### 4. Verify Azure AD App Registration

Ensure your Service Principal is properly configured:

1. Go to **Azure Portal** → **Azure Active Directory** → **App registrations**
2. Find your app (Client ID: `4ad7f68d-7193-4743-bb33-98640ef42841`)
3. Verify **API permissions**:
   - Click **API permissions**
   - Should include: `Power BI Service` → `Dataset.ReadWrite.All` (or `Dataset.Read.All`)
   - If not present, add it:
     - Click **Add a permission**
     - Select **Power BI Service**
     - Select **Application permissions**
     - Check **Dataset.ReadWrite.All**
     - Click **Add permissions**
     - Click **Grant admin consent** (requires admin)

### 5. Wait for Propagation

After making changes:
- Wait **5-10 minutes** for permissions to propagate
- The token cache in the application uses a 1-hour expiry, so new tokens will pick up new permissions

## Testing the Fix

After completing the steps above, test the connection:

```bash
python main.py
```

Then ask a simple question to verify the connection works.

## Matching Report Context (Optional but Recommended)

If API results do not match what you see in Power BI visuals, set one of these in `.env`:

```env
POWERBI_SERVER_NAME_FILTER="yourserver.ecolane.com"
```

- `POWERBI_SERVER_NAME_FILTER`: Explicitly applies one or more server names (comma-separated).

This helps align service-principal queries with report user context where RLS is based on `USERPRINCIPALNAME()`.
For production service-principal mode, do not set `POWERBI_SERVER_NAME_FILTER` unless you want to scope the dataset to specific servers.

## Common Issues

### Issue: "Cannot find Service Principal"
- **Solution**: Use the full Client ID, not the display name
- **Alternative**: Add the Service Principal to an Azure AD security group first, then add the group

### Issue: "Still getting 401 after adding to workspace"
- **Solution**: Ensure the workspace role is **Member** or **Contributor**, not just **Viewer**
- **Check**: Verify tenant settings allow Service Principals for your organization/group

### Issue: "Permissions not working"
- **Solution**: Clear the token cache by restarting the application
- **Check**: Verify the Service Principal has been granted admin consent in Azure AD

## Alternative: Use User Authentication (Temporary)

If Service Principal setup is blocked by organization policies, you can temporarily use user authentication:

1. This requires interactive login and is not recommended for production
2. Contact your Power BI admin to enable Service Principal access

## Need Help?

If issues persist:
1. Check Power BI Admin Portal audit logs
2. Verify in Azure AD that the app registration is active
3. Confirm with your Power BI admin that Service Principals are allowed
4. Test the Service Principal with a simpler Power BI REST API call first

## References

- [Power BI REST API - Service Principals](https://learn.microsoft.com/en-us/power-bi/developer/embedded/embed-service-principal)
- [Enable Service Principal Authentication](https://learn.microsoft.com/en-us/power-bi/admin/service-admin-portal-developer)
- [Workspace Roles](https://learn.microsoft.com/en-us/power-bi/collaborate-share/service-roles-new-workspaces)
