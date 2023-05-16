SET
    ANSI_NULLS ON
GO
SET
    QUOTED_IDENTIFIER ON
GO
    CREATE TABLE [dbo].[T019_GoogleAdGroup](
        [C019_QueryName] [varchar](50) NULL,
        [C019_Date] [date] NULL,
        [C019_Year] [varchar](255) NULL,
        [C019_YearMonth] [varchar](255) NULL,
        [C019_Month] [varchar](255) NULL,
        [C019_DayofMonth] [varchar](255) NULL,
        [C019_DayofWeek] [varchar](255) NULL,
        [C019_AdGroupName] [varchar](255) NULL,
        [C019_AdGroupID] [varchar](255) NULL,
        [C019_Account] [varchar](255) NULL,
        [C019_AccountID] [varchar](255) NULL,
        [C019_Network] [varchar](255) NULL,
        [C019_CampaignName] [varchar](255) NULL,
        [C019_CampaignID] [varchar](255) NULL,
        [C019_CampaignStatus] [varchar](255) NULL,
        [C019_BiddingStrategy] [varchar](255) NULL,
        [C019_AdGroupStatus] [varchar](255) NULL,
        [C019_MaxCpa] [varchar](255) NULL,
        [C019_MaxCpm] [varchar](255) NULL,
        [C019_CustomUrlParameters] [varchar](255) NULL,
        [C019_CampaignLabels] [varchar](255) NULL,
        [C019_AdGroupLabels] [varchar](255) NULL,
        [C019_Labels] [varchar](255) NULL,
        [C019_Impressions] [float] NULL,
        [C019_Clicks] [float] NULL,
        [C019_Cost] [float] NULL,
        [C019_Ctr] [varchar](255) NULL,
        [C019_Cpc] [float] NULL,
        [C019_Cpm] [float] NULL,
        [C019_Conversions] [float] NULL,
        [C019_ConversionRate] [varchar](255) NULL,
        [C019_ConversionsPerImpression] [varchar](255) NULL,
        [C019_CostPerConversion] [float] NULL,
        [C019_ValuePerConversion] [float] NULL,
        [C019_TotalConversionValue] [float] NULL,
        [C019_ReturnOnAdSpend] [float] NULL,
        [C019_ViewThroughConversions] [float] NULL,
        [C019_AllConversions] [float] NULL,
        [C019_AllConversionValue] [float] NULL,
        [C019_VideoImpressions] [float] NULL,
        [C019_VideoViews] [float] NULL,
        [C019_PhoneCallImpressions] [float] NULL,
        [C019_Calls] [float] NULL,
        [C019_CallRate] [varchar](255) NULL,
        [C019_GmailForwards] [float] NULL,
        [C019_GmailSaves] [float] NULL,
        [C019_GmailSecondaryClicks] [float] NULL,
        [C019_ImpressionShare] [float] NULL,
        [C019_SearchImpressionShare] [varchar](255) NULL,
        [C019_AvailableImpressions] [float] NULL,
        [C019_TopImpressionPercentage] [varchar](255) NULL,
        [C019_AbsoluteTopImpressionPercentage] [varchar](255) NULL,
        [C019_SearchAbsoluteTopImpressionShare] [varchar](255) NULL,
        [C019_SearchTopImpressionShare] [varchar](255) NULL,
        [C019_Mkwid] [varchar](255) NULL,
        [C019_Pubcode] [varchar](255) NULL,
        [DateRecordAddedToTable] [datetime2](7) NULL,
        [C019_Adkey] [varchar](255) NULL
    ) ON [PRIMARY]
GO
ALTER TABLE
    [dbo].[T019_GoogleAdGroup]
ADD
    CONSTRAINT [DF__T019_Goog__DateR__589C25F3] DEFAULT (getdate()) FOR [DateRecordAddedToTable]
GO